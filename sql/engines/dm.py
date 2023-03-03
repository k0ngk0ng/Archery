# -*- coding: UTF-8 -*-
import logging
import traceback
import re
import sqlparse
import simplejson as json
import threading
import pandas as pd
from common.config import SysConfig
from common.utils.timer import FuncTimer
from sql.utils.sql_utils import (
    get_syntax_type,
    get_full_sqlitem_list,
    get_exec_sqlitem_list,
)
from . import EngineBase
import dmPython
from .models import ResultSet, ReviewSet, ReviewResult
from sql.utils.data_masking import simple_column_mask

logger = logging.getLogger("default")


class DMEngine(EngineBase):
    test_query = "SELECT 1 FROM DUAL"

    def __init__(self, instance=None):
        super(DMEngine, self).__init__(instance=instance)

    def get_connection(self, db_name=None):
        if self.conn:
            return self.conn
        
        return dmPython.connect(
            server=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
        )

    @property
    def name(self):
        return "DM"

    @property
    def info(self):
        return "DM engine"

    @property
    def auto_backup(self):
        """是否支持备份"""
        return True
    
    @property
    def server_version(self):
        conn = self.get_connection()
        version = conn.server_version
        return tuple([n for n in version.split(".")[:3]])

    def get_all_databases(self):
        """获取数据库列表， 返回resultSet 供上层调用， 底层实际上是获取的schema列表"""
        result = self.query(sql="select owner from dba_objects where object_type='SCH' order by owner")
        sysschema = (
            "AUD_SYS",
            "ANONYMOUS",
            "APEX_030200",
            "APEX_PUBLIC_USER",
            "APPQOSSYS",
            "BI USERS",
            "CTXSYS",
            "DBSNMP",
            "DIP USERS",
            "EXFSYS",
            "FLOWS_FILES",
            "HR USERS",
            "IX USERS",
            "MDDATA",
            "MDSYS",
            "MGMT_VIEW",
            "OE USERS",
            "OLAPSYS",
            "ORACLE_OCM",
            "ORDDATA",
            "ORDPLUGINS",
            "ORDSYS",
            "OUTLN",
            "OWBSYS",
            "OWBSYS_AUDIT",
            "PM USERS",
            "SCOTT",
            "SH USERS",
            "SI_INFORMTN_SCHEMA",
            "SPATIAL_CSW_ADMIN_USR",
            "SPATIAL_WFS_ADMIN_USR",
            "SYS",
            "SYSMAN",
            "SYSTEM",
            "WMSYS",
            "XDB",
            "XS$NULL",
            "DIP",
            "OJVMSYS",
            "LBACSYS",
        )
        schema_list = [row[0] for row in result.rows if row[0] not in sysschema]
        result.rows = schema_list
        return result

    def get_all_tables(self, db_name, **kwargs):
        """获取table 列表, 返回一个ResultSet"""
        sql = f"""select TABLE_NAME from dba_tables where owner='{db_name}'
        """
        result = self.query(db_name=db_name, sql=sql)
        tb_list = [row[0] for row in result.rows]
        result.rows = tb_list
        return result

    def get_all_columns_by_tb(self, db_name, tb_name, **kwargs):
        """获取所有字段, 返回一个ResultSet"""
        sql = f"""select * from all_tab_columns where owner='{db_name}' and Table_Name='{tb_name}'
        """
        result = self.query(db_name=db_name, sql=sql)
        column_list = [row[0] for row in result.rows]
        result.rows = column_list
        return result
    
    def get_group_tables_by_db(self, db_name):
        data = {}
        table_list_sql = f"""SELECT table_name, comments FROM dba_tab_comments WHERE owner='{db_name}'"""
        result = self.query(db_name=db_name, sql=table_list_sql)
        for row in result.rows:
            table_name, table_cmt = row[0], row[1]
            if table_name[0] not in data:
                data[table_name[0]] = list()
            data[table_name[0]].append([table_name, table_cmt])
        return data

    def get_table_meta_data(self, db_name, tb_name, **kwargs):
        """数据字典页面使用：获取表格的元信息，返回一个dict{column_list: [], rows: []}"""
        meta_data_sql = f"""select      tcs.TABLE_NAME, --表名
                                        tcs.COMMENTS, --表注释
                                        tcs.TABLE_TYPE,  --表/试图 table/view
                                        ss.SEGMENT_TYPE,  --段类型 堆表/分区表/IOT表
                                        ts.TABLESPACE_NAME, --表空间
                                        ts.COMPRESSION, --压缩属性
                                        bss.NUM_ROWS, --表中的记录数
                                        bss.BLOCKS, --表中数据所占的数据块数
                                        bss.EMPTY_BLOCKS, --表中的空块数
                                        bss.AVG_SPACE, --数据块中平均的使用空间
                                        bss.CHAIN_CNT, --表中行连接和行迁移的数量
                                        bss.AVG_ROW_LEN, --每条记录的平均长度
                                        bss.LAST_ANALYZED  --上次统计信息搜集的时间
                                    from dba_tab_comments tcs
                                    left join dba_segments ss
                                        on ss.owner = tcs.OWNER
                                        and ss.segment_name = tcs.TABLE_NAME
                                    left join dba_tables ts
                                        on ts.OWNER = tcs.OWNER
                                        and ts.TABLE_NAME = tcs.TABLE_NAME
                                    left join DBA_TAB_STATISTICS bss
                                        on bss.TABLE_OWNER = tcs.owner
                                        and bss.TABLE_NAME = tcs.table_name
    
                                    WHERE
                                        tcs.OWNER='{db_name}'
                                        AND tcs.TABLE_NAME='{tb_name}'"""
        _meta_data = self.query(db_name=db_name, sql=meta_data_sql)
        return {"column_list": _meta_data.column_list, "rows": _meta_data.rows[0]}

    def get_table_desc_data(self, db_name, tb_name, **kwargs):
        """获取表格字段信息"""
        desc_sql = f"""SELECT bcs.COLUMN_NAME "列名",
                            ccs.comments "列注释" ,
                            bcs.data_type || case
                             when bcs.data_precision is not null and nvl(data_scale, 0) > 0 then
                              '(' || bcs.data_precision || ',' || data_scale || ')'
                             when bcs.data_precision is not null and nvl(data_scale, 0) = 0 then
                              '(' || bcs.data_precision || ')'
                             when bcs.data_precision is null and data_scale is not null then
                              '(*,' || data_scale || ')'
                             when bcs.char_length > 0 then
                              '(' || bcs.char_length || case char_used
                                when 'B' then
                                 ' Byte'
                                when 'C' then
                                 ' Char'
                                else
                                 null
                              end || ')'
                            end "字段类型",
                            bcs.DATA_DEFAULT "字段默认值",
                            decode(nullable, 'N', ' NOT NULL') "是否为空",
                            ics.INDEX_NAME "所属索引",
                            acs.constraint_type "约束类型"
                        FROM  dba_tab_columns bcs
                        left  join dba_col_comments ccs
                            on  bcs.OWNER = ccs.owner
                            and  bcs.TABLE_NAME = ccs.table_name
                            and  bcs.COLUMN_NAME = ccs.column_name
                        left  join dba_ind_columns ics
                            on  bcs.OWNER = ics.TABLE_OWNER
                            and  bcs.TABLE_NAME = ics.table_name
                            and  bcs.COLUMN_NAME = ics.column_name
                        left join dba_constraints acs
                            on acs.owner = ics.TABLE_OWNER
                            and acs.table_name = ics.TABLE_NAME
                            and acs.index_name = ics.INDEX_NAME
                        WHERE
                            bcs.OWNER='{db_name}'
                            AND bcs.TABLE_NAME='{tb_name}'
                        ORDER BY bcs.COLUMN_NAME"""
        _desc_data = self.query(db_name=db_name, sql=desc_sql)
        return {"column_list": _desc_data.column_list, "rows": _desc_data.rows}

    def get_table_index_data(self, db_name, tb_name, **kwargs):
        """获取表格索引信息"""
        index_sql = f""" SELECT 
	                            ais.INDEX_NAME "索引名称",
                                ais.uniqueness "唯一性",
                                ais.index_type "索引类型",
                                ais.compression "压缩属性",
                                ais.tablespace_name "表空间",
                                ais.status "状态",
                                ais.partitioned "分区" 
                            FROM DBA_INDEXES ais
                            WHERE
                                ais.owner = '{db_name}'
                                AND ais.table_name = '{tb_name}'"""
        _index_data = self.query(db_name, index_sql)
        return {"column_list": _index_data.column_list, "rows": _index_data.rows}

    def get_tables_metas_data(self, db_name, **kwargs):
        """获取数据库所有表格信息，用作数据字典导出接口"""
        table_metas = []
        sql_cols = f""" SELECT bcs.TABLE_NAME TABLE_NAME,
                                   tcs.COMMENTS TABLE_COMMENTS,
                                   bcs.COLUMN_NAME COLUMN_NAME,
                                   bcs.data_type || case
                                     when bcs.data_precision is not null and nvl(data_scale, 0) > 0 then
                                      '(' || bcs.data_precision || ',' || data_scale || ')'
                                     when bcs.data_precision is not null and nvl(data_scale, 0) = 0 then
                                      '(' || bcs.data_precision || ')'
                                     when bcs.data_precision is null and data_scale is not null then
                                      '(*,' || data_scale || ')'
                                     when bcs.char_length > 0 then
                                      '(' || bcs.char_length || case char_used
                                        when 'B' then
                                         ' Byte'
                                        when 'C' then
                                         ' Char'
                                        else
                                         null
                                      end || ')'
                                   end data_type,
                                   bcs.DATA_DEFAULT,
                                   decode(nullable, 'N', ' NOT NULL') nullable,
                                   t1.index_name,
                                   lcs.comments comments
                              FROM dba_tab_columns bcs
                              left join dba_col_comments lcs
                                on bcs.OWNER = lcs.owner
                               and bcs.TABLE_NAME = lcs.table_name
                               and bcs.COLUMN_NAME = lcs.column_name
                              left join dba_tab_comments tcs
                                on bcs.OWNER = tcs.OWNER
                               and bcs.TABLE_NAME = tcs.TABLE_NAME
                              left join (select acs.OWNER,
                                                acs.TABLE_NAME,
                                                scs.column_name,
                                                acs.index_name
                                           from dba_cons_columns scs
                                           join dba_constraints acs
                                             on acs.constraint_name = scs.constraint_name
                                            and acs.owner = scs.OWNER
                                          where acs.constraint_type = 'P') t1
                                on t1.OWNER = bcs.OWNER
                               AND t1.TABLE_NAME = bcs.TABLE_NAME
                               AND t1.column_name = bcs.COLUMN_NAME
                             WHERE bcs.OWNER = '{db_name}'
                             order by bcs.TABLE_NAME, comments"""
        cols_req = self.query(sql=sql_cols, close_conn=False).rows

        # 给查询结果定义列名，query_engine.query的游标是0 1 2
        cols_df = pd.DataFrame(
            cols_req,
            columns=[
                "TABLE_NAME",
                "TABLE_COMMENTS",
                "COLUMN_NAME",
                "COLUMN_TYPE",
                "COLUMN_DEFAULT",
                "IS_NULLABLE",
                "COLUMN_KEY",
                "COLUMN_COMMENT",
            ],
        )

        # 获得表名称去重
        col_list = cols_df.drop_duplicates("TABLE_NAME").to_dict("records")
        for cl in col_list:
            _meta = dict()
            engine_keys = [
                {"key": "COLUMN_NAME", "value": "字段名"},
                {"key": "COLUMN_TYPE", "value": "数据类型"},
                {"key": "COLUMN_DEFAULT", "value": "默认值"},
                {"key": "IS_NULLABLE", "value": "允许非空"},
                {"key": "COLUMN_KEY", "value": "是否主键"},
                {"key": "COLUMN_COMMENT", "value": "备注"},
            ]
            _meta["ENGINE_KEYS"] = engine_keys
            _meta["TABLE_INFO"] = {
                "TABLE_NAME": cl["TABLE_NAME"],
                "TABLE_COMMENTS": cl["TABLE_COMMENTS"],
            }
            table_name = cl["TABLE_NAME"]
            # 查询DataFrame中满足表名的记录，并转为list
            _meta["COLUMNS"] = cols_df.query("TABLE_NAME == @table_name").to_dict(
                "records"
            )

            table_metas.append(_meta)
        return table_metas

    def get_all_objects(self, db_name, **kwargs):
        """获取object_name 列表, 返回一个ResultSet"""
        sql = f"""SELECT object_name FROM all_objects WHERE OWNER = '{db_name}' """
        result = self.query(db_name=db_name, sql=sql)
        tb_list = [row[0] for row in result.rows]
        result.rows = tb_list
        return result

    def object_name_check(self, db_name=None, object_name=""):
        """获取table 列表, 返回一个ResultSet"""
        if "." in object_name:
            schema_name = object_name.split(".")[0]
            object_name = object_name.split(".")[1]
            if '"' in schema_name:
                schema_name = schema_name.replace('"', "")
                if '"' in object_name:
                    object_name = object_name.replace('"', "")
                else:
                    object_name = object_name.upper()
            else:
                schema_name = schema_name.upper()
                if '"' in object_name:
                    object_name = object_name.replace('"', "")
                else:
                    object_name = object_name.upper()
        else:
            schema_name = db_name
            if '"' in object_name:
                object_name = object_name.replace('"', "")
            else:
                object_name = object_name.upper()
        sql = f""" SELECT object_name FROM all_objects WHERE OWNER = '{schema_name}' and OBJECT_NAME = '{object_name}' """
        result = self.query(db_name=db_name, sql=sql, close_conn=False)
        if result.affected_rows > 0:
            return True
        else:
            return False

    @staticmethod
    def get_sql_first_object_name(sql=""):
        """获取sql文本中的object_name"""
        object_name = ""
        if re.match(r"^create\s+table\s", sql, re.M | re.IGNORECASE):
            object_name = re.match(
                r"^create\s+table\s(.+?)(\s|\()", sql, re.M | re.IGNORECASE
            ).group(1)
        elif re.match(r"^create\s+index\s", sql, re.M | re.IGNORECASE):
            object_name = re.match(
                r"^create\s+index\s(.+?)\s", sql, re.M | re.IGNORECASE
            ).group(1)
        elif re.match(r"^create\s+unique\s+index\s", sql, re.M | re.IGNORECASE):
            object_name = re.match(
                r"^create\s+unique\s+index\s(.+?)\s", sql, re.M | re.IGNORECASE
            ).group(1)
        elif re.match(r"^create\s+sequence\s", sql, re.M | re.IGNORECASE):
            object_name = re.match(
                r"^create\s+sequence\s(.+?)(\s|$)", sql, re.M | re.IGNORECASE
            ).group(1)
        elif re.match(r"^alter\s+table\s", sql, re.M | re.IGNORECASE):
            object_name = re.match(
                r"^alter\s+table\s(.+?)\s", sql, re.M | re.IGNORECASE
            ).group(1)
        elif re.match(r"^create\s+function\s", sql, re.M | re.IGNORECASE):
            object_name = re.match(
                r"^create\s+function\s(.+?)(\s|\()", sql, re.M | re.IGNORECASE
            ).group(1)
        elif re.match(r"^create\s+view\s", sql, re.M | re.IGNORECASE):
            object_name = re.match(
                r"^create\s+view\s(.+?)\s", sql, re.M | re.IGNORECASE
            ).group(1)
        elif re.match(r"^create\s+procedure\s", sql, re.M | re.IGNORECASE):
            object_name = re.match(
                r"^create\s+procedure\s(.+?)\s", sql, re.M | re.IGNORECASE
            ).group(1)
        elif re.match(r"^create\s+package\s+body", sql, re.M | re.IGNORECASE):
            object_name = re.match(
                r"^create\s+package\s+body\s(.+?)\s", sql, re.M | re.IGNORECASE
            ).group(1)
        elif re.match(r"^create\s+package\s", sql, re.M | re.IGNORECASE):
            object_name = re.match(
                r"^create\s+package\s(.+?)\s", sql, re.M | re.IGNORECASE
            ).group(1)
        else:
            return object_name.strip()
        return object_name.strip()

    @staticmethod
    def check_create_index_table(sql="", object_name_list=None, db_name=""):
        schema_name = '"' + db_name + '"'
        object_name_list = object_name_list or set()
        if re.match(r"^create\s+index\s", sql):
            table_name = re.match(
                r"^create\s+index\s+.+\s+on\s(.+?)(\(|\s\()", sql, re.M
            ).group(1)
            if "." not in table_name:
                table_name = f"{schema_name}.{table_name}"
            table_name = table_name.upper()
            if table_name in object_name_list:
                return True
            else:
                return False
        elif re.match(r"^create\s+unique\s+index\s", sql):
            table_name = re.match(
                r"^create\s+unique\s+index\s+.+\s+on\s(.+?)(\(|\s\()", sql, re.M
            ).group(1)
            if "." not in table_name:
                table_name = f"{schema_name}.{table_name}"
            table_name = table_name.upper()
            if table_name in object_name_list:
                return True
            else:
                return False
        else:
            return False

    @staticmethod
    def get_dml_table(sql="", object_name_list=None, db_name=""):
        schema_name = '"' + db_name + '"'
        object_name_list = object_name_list or set()
        if re.match(r"^update", sql):
            table_name = re.match(r"^update\s(.+?)\s", sql, re.M).group(1)
            if "." not in table_name:
                table_name = f"{schema_name}.{table_name}"
            table_name = table_name.upper()
            if table_name in object_name_list:
                return True
            else:
                return False
        elif re.match(r"^delete", sql):
            table_name = re.match(r"^delete\s+from\s+([\w-]+)\s*", sql, re.M).group(1)
            if "." not in table_name:
                table_name = f"{schema_name}.{table_name}"
            table_name = table_name.upper()
            if table_name in object_name_list:
                return True
            else:
                return False
        elif re.match(r"^insert\s", sql):
            table_name = re.match(
                r"^insert\s+((into)|(all\s+into)|(all\s+when\s(.+?)into))\s+(.+?)(\(|\s)",
                sql,
                re.M,
            ).group(6)
            if "." not in table_name:
                table_name = f"{schema_name}.{table_name}"
            table_name = table_name.upper()
            if table_name in object_name_list:
                return True
            else:
                return False
        else:
            return False

    @staticmethod
    def where_check(sql=""):
        if re.match(r"^update((?!where).)*$|^delete((?!where).)*$", sql):
            return True
        else:
            parsed = sqlparse.parse(sql)[0]
            flattened = list(parsed.flatten())
            n_skip = 0
            flattened = flattened[: len(flattened) - n_skip]
            logical_operators = (
                "AND",
                "OR",
                "NOT",
                "BETWEEN",
                "ORDER BY",
                "GROUP BY",
                "HAVING",
            )
            for t in reversed(flattened):
                if t.is_keyword:
                    return True
            return False    

    def query_check(self, db_name=None, sql=""):
        # 查询语句的检查、注释去除、切分
        result = {"msg": "", "bad_query": False, "filtered_sql": sql, "has_star": False}
        keyword_warning = ""
        star_patter = r"(^|,|\s)\*(\s|\(|$)"
        # 删除注释语句，进行语法判断，执行第一条有效sql
        try:
            sql = sqlparse.format(sql, strip_comments=True)
            sql = sqlparse.split(sql)[0]
            result["filtered_sql"] = re.sub(r";$", "", sql.strip())
            sql_lower = sql.lower()
        except IndexError:
            result["bad_query"] = True
            result["msg"] = "没有有效的SQL语句"
            return result
        if re.match(r"^select|^with|^explain", sql_lower) is None:
            result["bad_query"] = True
            result["msg"] = "不支持语法!"
            return result
        if re.search(star_patter, sql_lower) is not None:
            keyword_warning += "禁止使用 * 关键词\n"
            result["has_star"] = True
        if result.get("bad_query") or result.get("has_star"):
            result["msg"] = keyword_warning
        return result

    def filter_sql(self, sql="", limit_num=0):
        sql_lower = sql.lower()
        # 对查询sql增加limit限制
        if re.match(r"^select", sql_lower):
            if sql_lower.find(" top ") == -1:
                return sql_lower.replace("select", "select top {}".format(limit_num))
        return sql.strip()

    def query(self, db_name=None, sql="", limit_num=0, close_conn=True, **kwargs):
        """返回 ResultSet"""
        result_set = ResultSet(full_sql=sql)
        try:
            conn = self.get_connection()
            cursor = conn.cursor()            
            cursor.execute(sql)
            if int(limit_num) > 0:
                rows = cursor.fetchmany(int(limit_num))
            else:
                rows = cursor.fetchall()
            fields = cursor.description

            result_set.column_list = [i[0] for i in fields] if fields else []
            result_set.rows = [tuple(x) for x in rows]
            result_set.affected_rows = len(result_set.rows)
        except Exception as e:
            logger.warning(f"DM 语句执行报错，语句：{sql}，错误信息{traceback.format_exc()}")
            result_set.error = str(e)
        finally:
            if close_conn:
                self.close()
        return result_set

    def query_masking(self, db_name=None, sql="", resultset=None):
        """简单字段脱敏规则, 仅对select有效"""
        if re.match(r"^select", sql, re.I):
            filtered_result = simple_column_mask(self.instance, resultset)
            filtered_result.is_masked = True
        else:
            filtered_result = resultset
        return filtered_result

    def execute_check(self, db_name=None, sql="", close_conn=True):
        """
        上线单执行前的检查, 返回Review set
        使用explain对数据修改预计进行检测
        """
        config = SysConfig()
        check_result = ReviewSet(full_sql=sql)
        # explain支持的语法
        explain_re = r"^merge|^update|^delete|^insert|^create\s+table|^create\s+index|^create\s+unique\s+index"
        # 禁用/高危语句检查
        line = 1
        # 保存SQL中的新建对象
        object_name_list = set()
        critical_ddl_regex = config.get("critical_ddl_regex", "")
        p = re.compile(critical_ddl_regex)
        check_result.syntax_type = 2  # TODO 工单类型 0、其他 1、DDL，2、DML
        try:
            sqlitemList = get_full_sqlitem_list(sql, db_name)
            for sqlitem in sqlitemList:
                sql_lower = sqlitem.statement.lower().rstrip(";")
                sql_nolower = sqlitem.statement.rstrip(";")
                object_name = self.get_sql_first_object_name(sql=sql_lower)
                if "." in object_name:
                    object_name = object_name
                else:
                    object_name = f"""{db_name}.{object_name}"""
                object_name_list.add(object_name)
                # 禁用语句
                if re.match(r"^select|^with|^explain", sql_lower):
                    result = ReviewResult(
                        id=line,
                        errlevel=2,
                        stagestatus="驳回不支持语句",
                        errormessage="仅支持DML和DDL语句，查询语句请使用SQL查询功能！",
                        sql=sqlitem.statement,
                    )
                # 高危语句
                elif critical_ddl_regex and p.match(sql_lower.strip()):
                    result = ReviewResult(
                        id=line,
                        errlevel=2,
                        stagestatus="驳回高危SQL",
                        errormessage="禁止提交匹配" + critical_ddl_regex + "条件的语句！",
                        sql=sqlitem.statement,
                    )
                # 驳回未带where数据修改语句，如确实需做全部删除或更新，显示的带上where 1=1
                elif re.match(
                    r"^update((?!where).)*$|^delete((?!where).)*$", sql_lower
                ):
                    result = ReviewResult(
                        id=line,
                        errlevel=2,
                        stagestatus="驳回未带where数据修改",
                        errormessage="数据修改需带where条件！",
                        sql=sqlitem.statement,
                    )
                # 驳回事务控制，会话控制SQL
                elif re.match(r"^set|^rollback|^exit", sql_lower):
                    result = ReviewResult(
                        id=line,
                        errlevel=2,
                        stagestatus="SQL中不能包含^set|^rollback|^exit",
                        errormessage="SQL中不能包含^set|^rollback|^exit",
                        sql=sqlitem.statement,
                    )

                # 通过explain对SQL做语法语义检查
                elif re.match(explain_re, sql_lower) and sqlitem.stmt_type == "SQL":
                    if self.check_create_index_table(
                        db_name=db_name,
                        sql=sql_lower,
                        object_name_list=object_name_list,
                    ):
                        result = ReviewResult(
                            id=line,
                            errlevel=1,
                            stagestatus="WARNING:新建表的新建索引语句暂无法检测！",
                            errormessage="WARNING:新建表的新建索引语句暂无法检测！",
                            stmt_type=sqlitem.stmt_type,
                            object_owner=sqlitem.object_owner,
                            object_type=sqlitem.object_type,
                            object_name=sqlitem.object_name,
                            sql=sqlitem.statement,
                        )
                    elif len(object_name_list) > 0 and self.get_dml_table(
                        db_name=db_name,
                        sql=sql_lower,
                        object_name_list=object_name_list,
                    ):
                        result = ReviewResult(
                            id=line,
                            errlevel=1,
                            stagestatus="WARNING:新建表的数据修改暂无法检测！",
                            errormessage="WARNING:新建表的数据修改暂无法检测！",
                            stmt_type=sqlitem.stmt_type,
                            object_owner=sqlitem.object_owner,
                            object_type=sqlitem.object_type,
                            object_name=sqlitem.object_name,
                            sql=sqlitem.statement,
                        )
                    else:
                        result_set = self.explain_check(
                            db_name=db_name, sql=sqlitem.statement, close_conn=False
                        )
                        if result_set["msg"]:
                            result = ReviewResult(
                                id=line,
                                errlevel=2,
                                stagestatus="explain语法检查未通过！",
                                errormessage=result_set["msg"],
                                sql=sqlitem.statement,
                            )
                        else:
                            # 对create table\create index\create unique index语法做对象存在性检测
                            if re.match(
                                r"^create\s+table|^create\s+index|^create\s+unique\s+index",
                                sql_lower,
                            ):
                                object_name = self.get_sql_first_object_name(
                                    sql=sql_nolower
                                )
                                # 保存create对象对后续SQL做存在性判断
                                if "." in object_name:
                                    schema_name = object_name.split(".")[0]
                                    object_name = object_name.split(".")[1]
                                    if '"' in schema_name:
                                        schema_name = schema_name
                                        if '"' not in object_name:
                                            object_name = object_name.upper()
                                    else:
                                        schema_name = schema_name.upper()
                                        if '"' not in object_name:
                                            object_name = object_name.upper()
                                else:
                                    schema_name = '"' + db_name + '"'
                                    if '"' not in object_name:
                                        object_name = object_name.upper()

                                object_name = f"""{schema_name}.{object_name}"""
                                if (
                                    self.object_name_check(
                                        db_name=db_name, object_name=object_name
                                    )
                                    or object_name in object_name_list
                                ):
                                    result = ReviewResult(
                                        id=line,
                                        errlevel=2,
                                        stagestatus=f"""{object_name}对象已经存在！""",
                                        errormessage=f"""{object_name}对象已经存在！""",
                                        sql=sqlitem.statement,
                                    )
                                else:
                                    object_name_list.add(object_name)
                                    if result_set["rows"] > 1000:
                                        result = ReviewResult(
                                            id=line,
                                            errlevel=1,
                                            stagestatus="影响行数大于1000，请关注",
                                            errormessage="影响行数大于1000，请关注",
                                            sql=sqlitem.statement,
                                            stmt_type=sqlitem.stmt_type,
                                            object_owner=sqlitem.object_owner,
                                            object_type=sqlitem.object_type,
                                            object_name=sqlitem.object_name,
                                            affected_rows=result_set["rows"],
                                            execute_time=0,
                                        )
                                    else:
                                        result = ReviewResult(
                                            id=line,
                                            errlevel=0,
                                            stagestatus="Audit completed",
                                            errormessage="None",
                                            sql=sqlitem.statement,
                                            stmt_type=sqlitem.stmt_type,
                                            object_owner=sqlitem.object_owner,
                                            object_type=sqlitem.object_type,
                                            object_name=sqlitem.object_name,
                                            affected_rows=result_set["rows"],
                                            execute_time=0,
                                        )
                            else:
                                if result_set["rows"] > 1000:
                                    result = ReviewResult(
                                        id=line,
                                        errlevel=1,
                                        stagestatus="影响行数大于1000，请关注",
                                        errormessage="影响行数大于1000，请关注",
                                        sql=sqlitem.statement,
                                        stmt_type=sqlitem.stmt_type,
                                        object_owner=sqlitem.object_owner,
                                        object_type=sqlitem.object_type,
                                        object_name=sqlitem.object_name,
                                        affected_rows=result_set["rows"],
                                        execute_time=0,
                                    )
                                else:
                                    result = ReviewResult(
                                        id=line,
                                        errlevel=0,
                                        stagestatus="Audit completed",
                                        errormessage="None",
                                        sql=sqlitem.statement,
                                        stmt_type=sqlitem.stmt_type,
                                        object_owner=sqlitem.object_owner,
                                        object_type=sqlitem.object_type,
                                        object_name=sqlitem.object_name,
                                        affected_rows=result_set["rows"],
                                        execute_time=0,
                                    )
                # 其它无法用explain判断的语句
                else:
                    # 对alter table做对象存在性检查
                    if re.match(r"^alter\s+table\s", sql_lower):
                        object_name = self.get_sql_first_object_name(sql=sql_nolower)
                        if "." in object_name:
                            schema_name = object_name.split(".")[0]
                            object_name = object_name.split(".")[1]
                            if '"' in schema_name:
                                schema_name = schema_name
                                if '"' not in object_name:
                                    object_name = object_name.upper()
                            else:
                                schema_name = schema_name.upper()
                                if '"' not in object_name:
                                    object_name = object_name.upper()
                        else:
                            schema_name = '"' + db_name + '"'
                            if '"' not in object_name:
                                object_name = object_name.upper()

                        object_name = f"""{schema_name}.{object_name}"""
                        if (
                            not self.object_name_check(
                                db_name=db_name, object_name=object_name
                            )
                            and object_name not in object_name_list
                        ):
                            result = ReviewResult(
                                id=line,
                                errlevel=2,
                                stagestatus=f"""{object_name}对象不存在！""",
                                errormessage=f"""{object_name}对象不存在！""",
                                sql=sqlitem.statement,
                            )
                        else:
                            result = ReviewResult(
                                id=line,
                                errlevel=1,
                                stagestatus="当前平台，此语法不支持审核！",
                                errormessage="当前平台，此语法不支持审核！",
                                sql=sqlitem.statement,
                                stmt_type=sqlitem.stmt_type,
                                object_owner=sqlitem.object_owner,
                                object_type=sqlitem.object_type,
                                object_name=sqlitem.object_name,
                                affected_rows=0,
                                execute_time=0,
                            )
                    # 对create做对象存在性检查
                    elif re.match(r"^create", sql_lower):
                        object_name = self.get_sql_first_object_name(sql=sql_nolower)
                        if "." in object_name:
                            schema_name = object_name.split(".")[0]
                            object_name = object_name.split(".")[1]
                            if '"' in schema_name:
                                schema_name = schema_name
                                if '"' not in object_name:
                                    object_name = object_name.upper()
                            else:
                                schema_name = schema_name.upper()
                                if '"' not in object_name:
                                    object_name = object_name.upper()
                        else:
                            schema_name = '"' + db_name + '"'
                            if '"' not in object_name:
                                object_name = object_name.upper()

                        object_name = f"""{schema_name}.{object_name}"""
                        if (
                            self.object_name_check(
                                db_name=db_name, object_name=object_name
                            )
                            or object_name in object_name_list
                        ):
                            result = ReviewResult(
                                id=line,
                                errlevel=2,
                                stagestatus=f"""{object_name}对象已经存在！""",
                                errormessage=f"""{object_name}对象已经存在！""",
                                sql=sqlitem.statement,
                            )
                        else:
                            object_name_list.add(object_name)
                            result = ReviewResult(
                                id=line,
                                errlevel=1,
                                stagestatus="当前平台，此语法不支持审核！",
                                errormessage="当前平台，此语法不支持审核！",
                                sql=sqlitem.statement,
                                stmt_type=sqlitem.stmt_type,
                                object_owner=sqlitem.object_owner,
                                object_type=sqlitem.object_type,
                                object_name=sqlitem.object_name,
                                affected_rows=0,
                                execute_time=0,
                            )
                    else:
                        result = ReviewResult(
                            id=line,
                            errlevel=1,
                            stagestatus="当前平台，此语法不支持审核！",
                            errormessage="当前平台，此语法不支持审核！",
                            sql=sqlitem.statement,
                            stmt_type=sqlitem.stmt_type,
                            object_owner=sqlitem.object_owner,
                            object_type=sqlitem.object_type,
                            object_name=sqlitem.object_name,
                            affected_rows=0,
                            execute_time=0,
                        )
                # 判断工单类型
                if get_syntax_type(sql=sqlitem.statement, db_type="dm") == "DDL":
                    check_result.syntax_type = 1
                check_result.rows += [result]
                line += 1
        except Exception as e:
            logger.warning(
                f"DM 语句执行报错，第{line}个SQL：{sqlitem.statement}，错误信息{traceback.format_exc()}"
            )
            check_result.error = str(e)
        finally:
            if close_conn:
                self.close()
        # 统计警告和错误数量
        for r in check_result.rows:
            if r.errlevel == 1:
                check_result.warning_count += 1
            if r.errlevel == 2:
                check_result.error_count += 1
        return check_result

    def execute_workflow(self, workflow, close_conn=True):
        return self.execute(
            db_name=workflow.db_name, sql=workflow.sqlworkflowcontent.sql_content
        )
   
    def execute(self, db_name=None, sql="", close_conn=True):
        """原生执行语句"""
        result = ResultSet(full_sql=sql)
        conn = self.get_connection(db_name=db_name)
        try:
            cursor = conn.cursor()
            for statement in sqlparse.split(sql):
                statement = statement.rstrip(";")
                cursor.execute(statement)
        except Exception as e:
            logger.warning(f"DM 语句执行报错，语句：{sql}，错误信息{traceback.format_exc()}")
            result.error = str(e)
        if close_conn:
            self.close()
        return result     
       
    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
