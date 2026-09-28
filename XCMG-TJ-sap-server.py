"""
SAP MCP Server — 基于 PyRFC，将 BAPI/RFC 暴露为 MCP 工具
"""
import os
import threading
from contextlib import contextmanager
from typing import Any

from fastmcp import FastMCP
from pyrfc import Connection, ABAPApplicationError, ABAPRuntimeError, LogonError

# ==================== 配置区域（全部需要替换） ====================

SAP_CONFIG = {
    "ashost": "10.1.4.42",      # SAP 应用服务器地址，如 "10.0.0.1"
    "sysnr":  "00",       # 系统编号，如 "00"
    "client": "800",      # 客户端，如 "100"
    "user":   "XJ0XXH",        # RFC 用户名
    "passwd": "zxcvbnm123",      # RFC 密码
    "lang":   "ZH",        # 语言，如 "ZH" 或 "EN"
}

# 连接池大小（并发 RFC 调用上限）
POOL_SIZE = 5            # 建议值：3~5

# 允许读取的表名白名单（安全限制，防止查询敏感表）
ALLOWED_TABLES = [
    "MARA",           # 例如 "MARA"（物料主数据）
    "VBAK",           # 例如 "VBAK"（销售订单头）
    "LIPS",           # 例如 "LIPS"（交货单行项目）
]

# ==================== 连接池管理 ====================

class SAPConnectionPool:
    """简单的 PyRFC 连接池，避免每次调用都新建连接"""

    def __init__(self, config: dict, size: int = 3):
        self._config = config
        self._size = size
        self._pool: list[Connection] = []
        self._lock = threading.Lock()
        self._create_connections()

    def _create_connections(self):
        for _ in range(self._size):
            self._pool.append(Connection(**self._config))

    @contextmanager
    def acquire(self):
        """获取一个连接，用完归还"""
        conn = None
        with self._lock:
            if self._pool:
                conn = self._pool.pop()
        if conn is None:
            conn = Connection(**self._config)
        try:
            yield conn
        finally:
            with self._lock:
                if len(self._pool) < self._size:
                    self._pool.append(conn)

    def close_all(self):
        with self._lock:
            for c in self._pool:
                try:
                    c.close()
                except Exception:
                    pass
            self._pool.clear()


_pool = SAPConnectionPool(SAP_CONFIG, size=POOL_SIZE)


# ==================== 工具返回格式标准化 ====================

def _format_bapi_return(return_table: list) -> dict:
    """
    标准化 BAPI RETURN 表。
    RETURN 表是 BAPIRET2 结构，字段包括 TYPE, MESSAGE, MESSAGE_V1~V4 等[reference:1]。
    TYPE: S=成功, E=错误, W=警告, I=信息, A=中止
    """
    messages = []
    has_error = False
    for msg in return_table or []:
        msg_type = msg.get("TYPE", "")
        text = msg.get("MESSAGE", "").strip()
        if msg_type == "E" or msg_type == "A":
            has_error = True
        messages.append({
            "type": msg_type,
            "message": text,
            "id": msg.get("ID", ""),
            "number": msg.get("NUMBER", ""),
        })
    return {
        "success": not has_error,
        "messages": messages,
        "has_error": has_error,
    }


def _safe_serialize(obj: Any) -> Any:
    """递归将 PyRFC 返回的不可序列化对象转为可 JSON 化结构"""
    if isinstance(obj, dict):
        return {k: _safe_serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe_serialize(i) for i in obj]
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    return str(obj)


# ==================== MCP Server 定义 ====================

mcp = FastMCP("sap-rfc-server")


# ---------- Tool 1：通用 BAPI 调用 ----------

@mcp.tool()
def call_bapi(
    bapi_name: str,
    import_params: dict | None = None,
    table_params: dict | None = None,
) -> dict:
    """
    调用任意 SAP BAPI/RFC 函数模块。

    当用户要求执行一个具体的 SAP 业务操作（如创建订单、查询物料、修改客户）时使用此工具。
    如果不确定 BAPI 的名称和参数，请先调用 describe_rfc 获取函数签名。

    Args:
        bapi_name: BAPI 或 RFC 函数模块名称，如 "BAPI_MATERIAL_GET_DETAIL"。
        import_params: 导入参数（标量或结构体），如 {"MATERIAL": "000000000000001000"}。
        table_params: 表参数，值为字典列表。每个 BAPI 的表参数名不同，
                      例如 BAPI_SALESORDER_CREATEFROMDAT2 的 "ORDER_ITEMS_IN"。

    Returns:
        {
          "success": bool,
          "data": {...},          # BAPI 返回的业务数据
          "return_messages": [...] # BAPI RETURN 表标准化结果
        }
    """
    import_params = import_params or {}
    table_params = table_params or {}

    try:
        with _pool.acquire() as conn:
            # 合并参数：table_params 的值必须是 list，不能是 dict
            kwargs = {**import_params}
            for key, value in table_params.items():
                if isinstance(value, dict):
                    kwargs[key] = [value]   # 单行也包成 list
                elif isinstance(value, list):
                    kwargs[key] = value
                else:
                    kwargs[key] = value

            result = conn.call(bapi_name, **kwargs)

            # 提取 RETURN 表
            return_table = result.pop("RETURN", None) or result.pop("return", None) or []

            return {
                "success": _format_bapi_return(return_table)["success"],
                "data": _safe_serialize(result),
                "return_messages": _format_bapi_return(return_table)["messages"],
            }

    except LogonError as e:
        return {"success": False, "error": f"SAP 登录失败: {e}"}
    except ABAPApplicationError as e:
        return {"success": False, "error": f"ABAP 应用错误: {e}"}
    except ABAPRuntimeError as e:
        return {"success": False, "error": f"ABAP 运行时错误: {e}"}
    except Exception as e:
        return {"success": False, "error": f"RFC 调用失败: {e}"}


# ---------- Tool 2：提交事务（写操作后必须调用） ----------

@mcp.tool()
def commit_transaction(wait: bool = True) -> dict:
    """
    提交 SAP 事务。

    当通过 call_bapi 执行了写操作（创建、修改、删除）且 BAPI 返回成功后，
    必须调用此工具才能真正将数据写入 SAP 数据库。[reference:3]
    如果 BAPI 返回了错误，则不应调用此工具，而应调用 rollback_transaction。

    Args:
        wait: 是否等待提交完成，默认为 True。

    Returns:
        {"success": bool, "messages": [...]}
    """
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


# ---------- Tool 3：回滚事务 ----------

@mcp.tool()
def rollback_transaction() -> dict:
    """
    回滚 SAP 事务。

    当写操作 BAPI 返回错误，或用户要求取消当前未提交的变更时调用。
    """
    try:
        with _pool.acquire() as conn:
            conn.call("BAPI_TRANSACTION_ROLLBACK")
            return {"success": True, "message": "事务已回滚"}
    except Exception as e:
        return {"success": False, "error": f"回滚事务失败: {e}"}


# ---------- Tool 4：读取 SAP 表（受白名单限制） ----------

@mcp.tool()
def read_table(
    table_name: str,
    fields: list[str] | None = None,
    where_conditions: list[str] | None = None,
    max_rows: int = 50,
) -> dict:
    """
    读取 SAP 透明表的数据（通过 RFC_READ_TABLE）。

    当用户要求查询 SAP 中的原始数据，且没有对应的 BAPI 可用时使用。
    只能查询白名单中的表。如果要查的表不在白名单中，请返回错误信息告知用户。

    Args:
        table_name: 表名，必须是大写，如 "MARA"。
        fields: 要返回的字段列表，如 ["MATNR", "MTART"]。为空则返回所有字段（可能很大）。
        where_conditions: WHERE 条件列表，每个元素是 SQL 片段，
                          如 ["MTART = 'FERT'", "ERSDA >= '20240101'"]。
        max_rows: 最大返回行数，默认 50，防止数据量过大。

    Returns:
        {"success": bool, "table": str, "columns": [...], "rows": [...], "total_rows": int}
    """
    if table_name.upper() not in ALLOWED_TABLES:
        return {
            "success": False,
            "error": f"表 {table_name} 不在允许查询的白名单中。允许的表：{ALLOWED_TABLES}",
        }

    # RFC_READ_TABLE 的 OPTIONS 参数需要特定格式：每行 "条件"[reference:4]
    options = []
    for cond in (where_conditions or []):
        options.append({"TEXT": cond})

    # FIELDS 参数指定要读取的字段
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

            # FIELDS 返回字段元数据
            columns = [f["FIELDNAME"] for f in result.get("FIELDS", [])]
            # DATA 返回原始字符串行，用 DELIMITER 分隔
            raw_rows = result.get("DATA", [])
            rows = []
            for r in raw_rows:
                values = r["WA"].split("|") if "WA" in r else []
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


# ---------- Tool 5：获取 RFC 函数签名 ----------

@mcp.tool()
def describe_rfc(function_name: str) -> dict:
    """
    获取一个 RFC 函数模块的参数签名（导入参数、导出参数、表参数）。

    在调用 call_bapi 之前，如果不确定函数需要哪些参数，先调用此工具。
    返回的参数名可以直接用于 call_bapi 的 import_params 和 table_params。

    Args:
        function_name: RFC 函数模块名称，如 "BAPI_SALESORDER_CREATEFROMDAT2"。

    Returns:
        {"success": bool, "import_params": [...], "export_params": [...], "table_params": [...]}
    """
    try:
        with _pool.acquire() as conn:
            desc = conn.get_function_description(function_name)

            def _param_list(params):
                return [
                    {
                        "name": p.name,
                        "type": p.type,
                        "optional": p.optional,
                        "structure": getattr(p, "record_length", None),
                    }
                    for p in params
                ]

            return {
                "success": True,
                "function": function_name,
                "import_params": _param_list(desc.parameters.get("IMPORT", [])),
                "export_params": _param_list(desc.parameters.get("EXPORT", [])),
                "table_params": _param_list(desc.parameters.get("TABLES", [])),
            }

    except Exception as e:
        return {"success": False, "error": f"获取函数签名失败: {e}"}


# ==================== 启动 ====================

if __name__ == "__main__":
    try:
        mcp.run(transport="stdio")   # "stdio" 或 "streamable-http"
    finally:
        _pool.close_all()