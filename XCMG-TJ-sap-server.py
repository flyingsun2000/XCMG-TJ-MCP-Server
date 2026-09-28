"""
SAP MCP Server — 通过 HTTP SOAP RFC 调用 BAPI/RFC（无需 pyrfc / NW RFC SDK）
"""
import re
import threading
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from typing import Any

import requests
from fastmcp import FastMCP

# ==================== 配置区域 ====================

SAP_CONFIG = {
    "ashost": "mysap.goodsap.cn",       # SAP 应用服务器地址
    "sysnr":  "04",               # 系统编号（用于推算默认端口）
    "client": "400",              # 客户端
    "user":   "KN827",
    "passwd": "flyingsun",
    "lang":   "ZH",
    # HTTP SOAP RFC 端口：一般 HTTP = 8000 + sysnr；HTTPS = 44300 + sysnr
    "port":   None,               # None 表示自动推算
    "https":  False,              # 使用 HTTPS 改为 True
}

POOL_SIZE = 5

ALLOWED_TABLES = ["MARA", "VBAK", "LIPS"]

# ==================== HTTP SOAP RFC 客户端 ====================

SOAP_NS = "urn:sap-com:document:sap:rfc:functions"


class HTTPRFCConnection:
    """通过 SAP 的 SOAP RFC 端点调用 RFC 函数，替代 pyrfc。"""

    def __init__(self, config: dict):
        scheme = "https" if config.get("https") else "http"
        sysnr = int(config["sysnr"])
        default_port = (44300 if scheme == "https" else 8000) + sysnr
        port = config.get("port") or default_port
        client = config["client"]
        self.url = f"{scheme}://{config['ashost']}:{port}/sap/bc/soap/rfc?sap-client={client}"
        self.session = requests.Session()
        self.session.auth = (config["user"], config["passwd"])
        self.session.verify = False
        self.session.headers.update({
            "Content-Type": "text/xml; charset=utf-8",
        })
        self.lang = config.get("lang", "EN")
        # 关闭 verify=False 的警告
        try:
            import urllib3
            urllib3.disable_warnings()
        except Exception:
            pass

    # ---------- 对外调用入口 ----------

    def call(self, func_name: str, **kwargs) -> dict:
        # 过滤掉 None 值参数
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        xml = self._build_xml(func_name, kwargs)
        resp = self.session.post(
            self.url,
            data=xml.encode("utf-8"),
            headers={"SOAPAction": f'"{SOAP_NS}:{func_name}"'},
            timeout=60,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"HTTP {resp.status_code} 调用 {func_name} 失败：{resp.text[:500]}"
            )
        return self._parse_xml(resp.text, func_name)

    # ---------- 构造请求 ----------

    def _build_xml(self, func_name: str, params: dict) -> str:
        body_items = []
        for k, v in params.items():
            body_items.append(self._encode(k, v, level=3))
        body = "\n".join(body_items)
        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<soapenv:Envelope '
            'xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
            f'xmlns:urn="{SOAP_NS}">\n'
            '  <soapenv:Header/>\n'
            '  <soapenv:Body>\n'
            f'    <urn:{func_name}>\n'
            f'{body}\n'
            f'    </urn:{func_name}>\n'
            '  </soapenv:Body>\n'
            '</soapenv:Envelope>'
        )

    def _encode(self, name: str, value, level: int) -> str:
        pad = "  " * level
        if isinstance(value, list):
            # 表参数：<NAME><item>...</item>...</NAME>
            rows = "".join(self._encode("item", item, level + 1) + "\n" for item in value)
            return f"{pad}<{name}>\n{rows}{pad}</{name}>"
        if isinstance(value, dict):
            inner = "".join(self._encode(k, v, level + 1) + "\n" for k, v in value.items())
            return f"{pad}<{name}>\n{inner}{pad}</{name}>"
        if isinstance(value, bool):
            text = "X" if value else ""
        elif value is None:
            text = ""
        else:
            text = str(value)
        text = (text.replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;"))
        return f"{pad}<{name}>{text}</{name}>"

    # ---------- 解析响应 ----------

    def _parse_xml(self, xml_text: str, func_name: str) -> dict:
        root = ET.fromstring(xml_text)
        body = self._find_child(root, "Body")
        if body is None:
            raise RuntimeError("SOAP 响应缺少 Body")

        fault = self._find_child(body, "Fault")
        if fault is not None:
            text = self._find_child(fault, "faultstring")
            msg = text.text if text is not None else ET.tostring(fault, encoding="unicode")
            raise RuntimeError(f"SOAP Fault: {msg}")

        # 找 <urn:FUNCNAME.Response>
        response = None
        for child in body:
            local = self._local_name(child.tag)
            if local.startswith(func_name):
                response = child
                break
        if response is None and len(body):
            response = body[0]
        return self._to_dict(response) if response is not None else {}

    def _to_dict(self, elem):
        if elem is None:
            return None
        children = list(elem)
        if not children:
            return (elem.text or "").strip()

        # 全部子元素同名 → 数组
        names = [self._local_name(c.tag) for c in children]
        if len(names) > 1 and len(set(names)) == 1:
            return [self._to_dict(c) for c in children]

        result: dict[str, Any] = {}
        for c in children:
            name = self._local_name(c.tag)
            val = self._to_dict(c)
            if name in result:
                if not isinstance(result[name], list):
                    result[name] = [result[name]]
                result[name].append(val)
            else:
                result[name] = val
        return result

    @staticmethod
    def _local_name(tag: str) -> str:
        return tag.split("}", 1)[-1] if "}" in tag else tag

    def _find_child(self, elem, local_name: str):
        if elem is None:
            return None
        for c in elem:
            if self._local_name(c.tag) == local_name:
                return c
        return None

    def close(self):
        try:
            self.session.close()
        except Exception:
            pass


# ==================== 连接池 ====================

class SAPConnectionPool:
    def __init__(self, config: dict, size: int = 3):
        self._config = config
        self._size = size
        self._pool: list[HTTPRFCConnection] = []
        self._lock = threading.Lock()
        for _ in range(size):
            self._pool.append(HTTPRFCConnection(config))

    @contextmanager
    def acquire(self):
        conn = None
        with self._lock:
            if self._pool:
                conn = self._pool.pop()
        if conn is None:
            conn = HTTPRFCConnection(self._config)
        try:
            yield conn
        finally:
            with self._lock:
                if len(self._pool) < self._size:
                    self._pool.append(conn)
                else:
                    conn.close()

    def close_all(self):
        with self._lock:
            for c in self._pool:
                c.close()
            self._pool.clear()


_pool = SAPConnectionPool(SAP_CONFIG, size=POOL_SIZE)


# ==================== 辅助函数 ====================

def _format_bapi_return(return_table) -> dict:
    messages = []
    has_error = False
    if isinstance(return_table, dict):
        return_table = [return_table]
    for msg in (return_table or []):
        if not isinstance(msg, dict):
            continue
        msg_type = msg.get("TYPE", "")
        text = (msg.get("MESSAGE") or "").strip()
        if msg_type in ("E", "A"):
            has_error = True
        messages.append({
            "type": msg_type,
            "message": text,
            "id": msg.get("ID", ""),
            "number": msg.get("NUMBER", ""),
        })
    return {"success": not has_error, "messages": messages, "has_error": has_error}


def _safe_serialize(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _safe_serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe_serialize(i) for i in obj]
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    return str(obj)


# ==================== MCP Server ====================

mcp = FastMCP("sap-rfc-server")


@mcp.tool()
def call_bapi(
    bapi_name: str,
    import_params: dict | None = None,
    table_params: dict | None = None,
) -> dict:
    """调用任意 SAP BAPI/RFC 函数模块。"""
    import_params = import_params or {}
    table_params = table_params or {}

    try:
        with _pool.acquire() as conn:
            kwargs = {**import_params}
            for key, value in table_params.items():
                if isinstance(value, dict):
                    kwargs[key] = [value]
                elif isinstance(value, list):
                    kwargs[key] = value
                else:
                    kwargs[key] = value

            result = conn.call(bapi_name, **kwargs)

            return_table = None
            for key in ("RETURN", "return", "Return"):
                if key in result:
                    return_table = result.pop(key)
                    break

            fmt = _format_bapi_return(return_table)
            return {
                "success": fmt["success"],
                "data": _safe_serialize(result),
                "return_messages": fmt["messages"],
            }
    except Exception as e:
        return {"success": False, "error": f"RFC 调用失败: {e}"}


@mcp.tool()
def commit_transaction(wait: bool = True) -> dict:
    """提交 SAP 事务。"""
    try:
        with _pool.acquire() as conn:
            result = conn.call("BAPI_TRANSACTION_COMMIT", WAIT=wait)
            return_table = result.get("RETURN", []) or []
            return {
                "success": _format_bapi_return(return_table)["success"],
                "messages": _format_bapi_return(return_table)["messages"],
            }
    except Exception as e:
        return {"success": False, "error": f"提交事务失败: {e}"}


@mcp.tool()
def rollback_transaction() -> dict:
    """回滚 SAP 事务。"""
    try:
        with _pool.acquire() as conn:
            conn.call("BAPI_TRANSACTION_ROLLBACK")
            return {"success": True, "message": "事务已回滚"}
    except Exception as e:
        return {"success": False, "error": f"回滚事务失败: {e}"}


@mcp.tool()
def read_table(
    table_name: str,
    fields: list[str] | None = None,
    where_conditions: list[str] | None = None,
    max_rows: int = 50,
) -> dict:
    """读取 SAP 透明表的数据（通过 RFC_READ_TABLE）。"""
    if table_name.upper() not in ALLOWED_TABLES:
        return {
            "success": False,
            "error": f"表 {table_name} 不在允许查询的白名单中。允许的表：{ALLOWED_TABLES}",
        }

    options = [{"TEXT": c} for c in (where_conditions or [])]
    field_list = [{"FIELDNAME": f} for f in (fields or [])]

    try:
        with _pool.acquire() as conn:
            result = conn.call(
                "RFC_READ_TABLE",
                QUERY_TABLE=table_name.upper(),
                DELIMITER="|",
                ROWCOUNT=max_rows,
                OPTIONS=options if options else None,
                FIELDS=field_list if field_list else None,
            )

            fields_resp = result.get("FIELDS") or []
            if isinstance(fields_resp, dict):
                fields_resp = [fields_resp]
            columns = [f.get("FIELDNAME", "") for f in fields_resp if isinstance(f, dict)]

            data_resp = result.get("DATA") or []
            if isinstance(data_resp, dict):
                data_resp = [data_resp]
            rows = []
            for r in data_resp:
                if not isinstance(r, dict):
                    continue
                wa = r.get("WA", "")
                values = wa.split("|") if wa else []
                rows.append(dict(zip(columns, values)))

            return {
                "success": True,
                "table": table_name.upper(),
                "columns": columns,
                "rows": rows,
                "total_rows": len(rows),
            }
    except Exception as e:
        return {"success": False, "error": f"读取表 {table_name} 失败: {e}"}


@mcp.tool()
def describe_rfc(function_name: str) -> dict:
    """
    获取 RFC 函数签名。

    注意：HTTP SOAP 方式没有直接的函数描述接口。
    建议在 SAP GUI 中用 SE37 查看参数签名，或调用
    RFC_GET_FUNCTION_INTERFACE 获取原始结构。
    """
    try:
        with _pool.acquire() as conn:
            result = conn.call("RFC_GET_FUNCTION_INTERFACE", FUNCNAME=function_name)
            return {
                "success": True,
                "function": function_name,
                "raw": _safe_serialize(result),
                "note": "请根据 raw 字段解析，或改用 SE37 查看签名。",
            }
    except Exception as e:
        return {"success": False, "error": f"获取函数签名失败: {e}"}


# ==================== 启动 ====================

if __name__ == "__main__":
    try:
        mcp.run(transport="stdio")
    finally:
        _pool.close_all()
