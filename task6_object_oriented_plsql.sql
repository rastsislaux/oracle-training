drop type refl_table;
drop type refl_column_t;
drop type refl_column;

create or replace type refl_column is object
(
    table_name  varchar(128),
    column_name varchar(128),

    constructor function refl_column(table_name varchar2, column_name varchar2) return self as result,
    member function get_data_type return varchar2,
    member function is_nullable return boolean
);

create or replace type body refl_column as
    constructor function refl_column(table_name varchar2, column_name varchar2) return self as result is
    begin
        self.table_name := upper(table_name);
        self.column_name := upper(column_name);
        return;
    end;

    member function get_data_type return varchar2 is
        v_data_type varchar(128);
    begin
        select data_type
        into v_data_type
        from user_tab_columns utc
        where utc.column_name = self.column_name
          and utc.table_name = self.table_name;
    end;

    member function is_nullable return boolean is
        v_is_nullable boolean;
    begin
        select case when utc.nullable = 'Y' then true else false end
        into v_is_nullable
        from user_tab_columns utc
        where utc.column_name = self.column_name
          and utc.table_name = self.table_name;
        return v_is_nullable;
    end;
end;

create or replace type refl_column_t is table of refl_column;

create or replace type refl_table is object
(
    table_name varchar(128),

    constructor function refl_table(table_name varchar2) return self as result,
    member function get_columns return refl_column_t
);

create or replace type body refl_table as
    constructor function refl_table(table_name varchar2) return self as result is
    begin
        self.table_name := upper(table_name);
        return;
    end;

    member function get_columns return refl_column_t is
        v_columns refl_column_t;
    begin
        select refl_column(self.table_name, cols.column_name)
        bulk collect into v_columns
        from user_tab_columns cols
        where cols.table_name = upper(self.table_name);

        return v_columns;
    end;
end;

create or replace package refl as
    function create_table(p_table_name varchar2) return refl_table;
end;

create or replace package body refl as
    function create_table(p_table_name varchar2) return refl_table as
        v_sql varchar2(32767);
    begin
        v_sql := 'create table ' || p_table_name || ' ( id number not null )';
        execute immediate v_sql;
        return refl_table(p_table_name);
    end;
end;

declare
    v_table refl_table;
    v_columns refl_column_t;
begin
    v_table := refl.create_table('test_table');
    v_columns := v_table.get_columns();
    dbms_output.put_line('List of columns for table: ' || v_table.table_name);
    for i in 1..v_columns.count loop
        dbms_output.put_line('Table: ' || v_columns(i).table_name || ', ' || v_columns(i).column_name);
        if not v_columns(i).is_nullable then
            DBMS_OUTPUT.put_line('nullable');
        end if;
    end loop;
end;

