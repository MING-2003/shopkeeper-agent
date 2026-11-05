import pytest

from app.core.sql_guard import SQLGuardError, guard_sql


@pytest.mark.parametrize(
    "sql",
    [
        "delete from dw_order_info",
        "update t set a = 1",
        "drop table t",
        "select 1; select 2",
        "select * from t into outfile '/tmp/x'",
        "select * from t into dumpfile '/tmp/x'",
    ],
)
def test_reject(sql):
    with pytest.raises(SQLGuardError):
        guard_sql(sql)


def test_add_limit():
    assert "LIMIT 1000" in guard_sql("select * from t").upper()


def test_cap_limit():
    assert "LIMIT 1000" in guard_sql("select * from t limit 99999999").upper()


def test_union_ok():
    guard_sql("select a from t1 union all select b from t2")


def test_cte_ok():
    guard_sql("with t as (select 1 as a) select a from t")
