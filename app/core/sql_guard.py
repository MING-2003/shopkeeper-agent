"""
SQL 安全防护

用 sqlglot 把 SQL 解析成 AST = Abstract Syntax Tree（抽象语法树）。 后做三件事：
1. 语句白名单：只放行单条 SELECT（含 CTE/UNION），拒绝 DML数据操作语言/DDL数据定义语言/多语句
2. 强制 LIMIT：无 LIMIT 自动补，已有 LIMIT 超过上限则压回
3. 还原为规范 SQL 文本，交给仓储层执行
Prompt 里"只生成 SELECT"是软约束，这里是程序性硬约束
"""

from sqlglot import expressions as exp
from sqlglot.errors import ParseError

import sqlglot

MAX_ROW_LIMIT = 1000


class SQLGuardError(Exception):
    """生成的 SQL 未通过安全防护"""


def guard_sql(sql: str, max_row_limit: int = MAX_ROW_LIMIT) -> str:
    """对 LLM 生成的 SQL 做安全防护，返回规范化后的安全 SQL

    任何不满足白名单的情况都抛出 SQLGuardError，由调用方决定如何处理
    （validate_sql 节点会捕获它写入 state["error"]，从而进入修正循环）
    """
    try:
        statements = sqlglot.parse(sql, read="mysql")
    except ParseError as e:
        # 解析失败同样视为未通过防护：无法理解的 SQL 一律不放行
        raise SQLGuardError(f"SQL 无法解析：{e}") from e

    # 多语句（如 "select 1; delete from ..."）直接拒绝
    if len(statements) != 1:
        raise SQLGuardError("只允许执行单条查询语句")

    stmt = statements[0]
    # CTE(WITH) 在 sqlglot 中也是 exp.Select；UNION 是 exp.Union
    if not isinstance(stmt, (exp.Select, exp.Union)):
        raise SQLGuardError("只允许执行 SELECT 查询")

    # SELECT ... INTO OUTFILE/DUMPFILE 的顶层节点同样是 exp.Select（INTO 挂在
    # args["into"] 上），上面的类型检查拦不住它，必须显式拒绝——否则可写任意服务器文件
    if stmt.args.get("into"):
        raise SQLGuardError("不允许 SELECT INTO OUTFILE/DUMPFILE")

    # LIMIT 只看最外层（对 UNION 整体生效）：若逐分支加 LIMIT，
    # 总行数上限会变成 分支数 × max_row_limit，与"强制行数上限"语义不符
    limit = stmt.args.get("limit")
    if limit is None:
        # 直接构造 Limit 节点写入 AST，不依赖 builder 方法在不同 sqlglot 版本间的签名差异
        stmt.set("limit", exp.Limit(expression=exp.Literal.number(max_row_limit)))
    else:
        literal = limit.expression
        if isinstance(literal, exp.Literal) and literal.is_int:
            if int(literal.name) > max_row_limit:
                limit.set("expression", exp.Literal.number(max_row_limit))

    return stmt.sql(dialect="mysql")
