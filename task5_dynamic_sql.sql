-- Вспомогательные функции и типы:

set serveroutput on;

-- Массив строк:
create or replace type strarr as table of varchar2(255);

-- Функция для проверки наличия в массиве строки по равенству или подобию:
create or replace function is_exist(p_arr strarr, p_type varchar2, p_e varchar2) return boolean is
    v_exists boolean := false;
begin
    for i in 1 .. p_arr.count
        loop
            if (p_type = 'equal' and p_arr(i) = p_e) OR (p_type = 'like' and p_arr(i) like p_e) then
                v_exists := true;
                exit;
            end if;
        end loop;
    return v_exists;
end;

-- Функция для объединения строк через разделитель
create or replace function join_strarr(p_arr strarr, p_delimiter varchar2) return varchar2 is
    v_result varchar2(4000) := '';
begin
    if p_arr is not null and p_arr.count > 0 then
        for i in 1 .. p_arr.count
            loop
                v_result := v_result || p_arr(i);
                if i < p_arr.COUNT then
                    v_result := v_result || p_delimiter;
                end if;
            end loop;
    end if;
    return v_result;
end;

-- Тип данных с информацией о столбце
create or replace type column_record is object
(
    column_name VARCHAR2(255),
    data_type   VARCHAR2(255),
    nullable    boolean
);

-- Функция для получения информации о столбце
create or replace function get_column_details(p_table varchar2, p_column varchar2) return column_record is
    v_column_record column_record;
begin
    select column_record(column_name, data_type, nullable)
    into v_column_record
    from user_tab_columns
    where table_name = upper(p_table)
      and column_name = upper(p_column);
    return v_column_record;
end;

-- Функция для проверки, существует ли таблица или представление
create or replace function object_exists(p_table varchar2) return boolean is
    v_table_exists boolean;
begin
    select count(*) > 0
    into v_table_exists
    from (select 1
          from user_tables
          where table_name = upper(p_table)
          union all
          select 1
          from user_views
          where view_name = upper(p_table));
    return v_table_exists;
end;

-- Функция для получения списка всех столбцов в таблице
create or replace function get_columns(p_table varchar2) return strarr is
    cursor v_columns_cursor is
        select column_name
        from user_tab_columns
        where table_name = upper(p_table);
    v_columns_list strarr := strarr();
begin
    for column in v_columns_cursor
        loop
            v_columns_list.extend;
            v_columns_list(v_columns_list.count) := column.column_name;
        end loop;

    return v_columns_list;
end;

-- Функция для проверки существования столбца
create or replace function column_exists(p_table varchar2, p_column varchar2) return boolean is
    v_column_exists boolean;
begin
    select count(*) > 0
    into v_column_exists
    from user_tab_columns
    where table_name = upper(p_table)
      and column_name = upper(p_column);
    return v_column_exists;
end;

-- 1.	Написать с помощью пакета DBMS_SQL динамическую процедуру или функцию, в которой заранее
-- неизвестен текст команды SELECT. Преду-смотреть возможность вывода разных результатов,
-- в зависимости от количе-ства передаваемых параметров.

create or replace function read_project_info(p_columns strarr) return sys_refcursor is
    v_cursor     integer;
    v_query      varchar2(4000);
    v_joins      strarr := strarr();
    v_status     integer;
    v_ref_cursor sys_refcursor;
begin
    v_cursor := dbms_sql.open_cursor;

    if is_exist(p_columns, 'like', 'client.%') then
        v_joins.extend;
        v_joins(v_joins.count) := 'left join lw1_client client on project.client_id = client.id';
    end if;

    if is_exist(p_columns, 'like', 'team.%') then
        v_joins.extend;
        v_joins(v_joins.count) := 'left join lw1_team team on project.team_id = team.id';
    end if;

    v_query := 'select ' || join_strarr(p_columns, ',') ||
               ' from lw1_project project ' || join_strarr(v_joins, ' ');
    dbms_output.put_line('Query: ' || v_query);

    dbms_sql.parse(v_cursor, v_query, dbms_sql.native);

    v_status := dbms_sql.execute(v_cursor);
    v_ref_cursor := dbms_sql.to_refcursor(v_cursor);

    return v_ref_cursor;
end;

-- Проверка
declare
    v_cursor sys_refcursor;
    type project_info is record(id int, client_name varchar2(1000), team_name varchar2(1000));
    pi project_info;
begin
    v_cursor := read_project_info(strarr('project.id', 'client.name', 'team.name'));
    loop
        fetch v_cursor into pi;
        exit when v_cursor%notfound;
        dbms_output.put_line(pi.id || ' | ' || pi.client_name || ' | ' || pi.team_name);
    end loop;
end;


-- 2.	Написать, используя встроенный динамический SQL, процедуру со-здания в БД нового объекта
-- (представления или таблицы) на основе суще-ствующей таблицы. Имя нового объекта должно
-- формироваться динамически и проверяться на существование в словаре данных. В качестве входных
-- пара-метров указать тип нового объекта, исходную таблицу, столбцы и количество строк, которые
-- будут использоваться в запросе.

create or replace function create_object(p_type varchar2, p_table varchar2, p_columns strarr,
                                         p_limit number) return varchar2 is
    v_source_column_list strarr;
    v_sql                varchar(32767);

    -- для вьюшки
    function build_create_query_for_view(p_table varchar2, p_columns strarr, p_limit number) return varchar2 is
        v_new_object_name varchar2(32767) := 'generated_view_' || p_table || '_' ||
                                             join_strarr(p_columns, '_') || '_' || p_limit;
    begin
        if object_exists(v_new_object_name) then
            raise_application_error(-20004,
                                    'Невозможно создать объект, поскольку представление с именем ' ||
                                    v_new_object_name || ' уже существует.');
        end if;

        return 'create view ' || v_new_object_name || ' as select ' ||
               join_strarr(p_columns, ',') || ' from ' || p_table || ' where rownum <= ' ||
               p_limit;
    end;

    -- для таблицы
    -- создаем таблицу
    function build_create_query_for_table(p_table varchar2, p_columns strarr, p_limit number) return varchar2 is
        v_new_object_name varchar2(32767) := 'generated_table_' || p_table || '_' ||
                                             join_strarr(p_columns, '_') || '_' || p_limit;
        v_column_data_one     column_record;
        v_nullable        varchar(15);
        v_column_data_many     strarr          := strarr();
    begin
        if object_exists(v_new_object_name) then
            raise_application_error(-20004,
                                    'Невозможно создать объект, поскольку таблица с именем ' ||
                                    v_new_object_name || ' уже существует.');
        end if;

        for i in 1 .. p_columns.count
            loop
                v_column_data_one := get_column_details(p_table, p_columns(i));
                if v_column_data_one.nullable then
                    v_nullable := '';
                else
                    v_nullable := ' NOT NULL';
                end if;
                v_column_data_many.extend;
                v_column_data_many(v_column_data_many.count) :=
                        v_column_data_one.column_name || ' ' || v_column_data_one.data_type ||
                        v_nullable;
            end loop;

        return 'create table ' || v_new_object_name || '(' || join_strarr(v_column_data_many, ',') ||
               ')';
    end;

    -- заполняем таблицу
    function build_insert_query_for_table(p_table varchar2, p_columns strarr, p_limit number) return varchar2 is
        v_new_object_name varchar2(32767) := 'generated_table_' || p_table || '_' ||
                                             join_strarr(p_columns, '_') || '_' || p_limit;
    begin
        return 'insert into ' || v_new_object_name || ' select ' || join_strarr(p_columns, ',') ||
               ' from ' || p_table || ' where rownum <=' || p_limit;
    end;

-- Тело функции
begin
    if (p_type not in ('table', 'view')) then
        raise_application_error(-20001, 'Некорректный тип создаваемого объекта: ' || p_type ||
                                        '. Допустимые значения: `table`, `view`');
    end if;

    if not object_exists(p_table) then
        raise_application_error(-20002, 'Исходный объект ' || p_table ||
                                        'не найден. Проверьте правильность написания названия объекта.');
    end if;

    v_source_column_list := get_columns(p_table);
    for i in 1 .. p_columns.count
    loop
        if not is_exist(v_source_column_list, 'equal', upper(p_columns(i))) then
            raise_application_error(-20003, 'Для объекта ' || p_table ||
                                            ' не существует столбца с именем ' ||
                                            p_columns(i) || '.');
        end if;
    end loop;

    if (p_type = 'table') then
        v_sql := build_create_query_for_table(p_table, p_columns, p_limit);
        dbms_output.put_line('Create table query: ' || v_sql);
        execute immediate v_sql;
        v_sql := build_insert_query_for_table(p_table, p_columns, p_limit);
        dbms_output.put_line('Insert query: ' || v_sql);
        execute immediate v_sql;
        return 'generated_table_' || p_table || '_' || join_strarr(p_columns, '_') || '_' || p_limit;

    else
        v_sql := build_create_query_for_view(p_table, p_columns, p_limit);
        dbms_output.put_line('Create view query: ' || v_sql);
        execute immediate v_sql;
        return 'generated_view_' || p_table || '_' || join_strarr(p_columns, '_') || '_' || p_limit;
    end if;

end;

--Пример!
declare
    v_object_name varchar2(32767);
begin
    v_object_name := create_object('view', 'lw1_project', strarr('id', 'start_date'), 10);
    dbms_output.put_line('Object name: ' || v_object_name);
end;

select *
from generated_view_lw1_project_id_start_date_10;

-- 3.2. Создать процедуру, которая принимает в качестве параметра имя таблицы и имя поля в этой
-- таблице. Процедура подсчитывает и выводит на экран статистику по этой таблице: количество
-- записей, имя поля, количество различных значений поля, количество null-значений.

create or replace procedure make_field_report(p_source varchar2, p_column varchar2) is
    v_record_count   number;
    v_distinct_count number;
    v_null_count     number;

begin
    if not object_exists(p_source) then
        raise_application_error(-20001, 'Таблица с именем ' || p_source || ' не существует.');
    end if;

    if not column_exists(p_source, p_column) then
        raise_application_error(-20002,
                                'В таблице ' || p_source || ' не существует столбца с именем ' ||
                                p_column);
    end if;

    execute immediate 'select count(*) from ' || p_source into v_record_count;
    execute immediate 'select count(distinct ' || p_column || ') from ' || p_source into v_distinct_count;
    execute immediate 'select count(case when ' || p_column || ' is null then 1 end) from ' ||
                      p_source into v_null_count;

    dbms_output.put_line('Отчет по таблице ' || p_source || ', столбец - ' || p_column || ':');
    dbms_output.put_line(' - Количество записей: ' || v_record_count);
    dbms_output.put_line(' - Количество уникальных значений столбца ' || p_column || ': ' ||
                         v_distinct_count);
    dbms_output.put_line(' - Количество null-значений столбца ' || p_column || ': ' ||
                         v_null_count);
end;

call make_field_report('lw1_client', 'email');

-- 4.	Написать программу, которая позволит для двух указанных в параметрах таблиц существующей
-- БД определить, есть ли между ними связь «один ко многим». Если связь есть, то на основе
-- родительской таблицы создать новую, в которой будут присутствовать все поля старой и одно
-- новое поле с типом коллекции, в котором при переносе данных помещаются все связанные записи из
-- дочерней таблицы.

create or replace procedure dump_relation(p_source_one varchar2, p_source_many varchar2) is
    v_fk               varchar2(100);
    v_columns          strarr;
    v_columns_prefixed strarr := strarr();
    v_sql              varchar2(32767);
    v_record_type_name varchar(32767);
    v_table_type_name  varchar(32767);

    function get_foreign_key_column(p_source_one varchar2, p_source_many varchar2) return varchar2 is
        v_constraint_name varchar2(100);
    begin
        select col.column_name
        into v_constraint_name
        from user_constraints fk
                 join user_constraints pk on fk.r_constraint_name = pk.constraint_name
                 join user_cons_columns col on fk.constraint_name = col.constraint_name
        where fk.constraint_type = 'R'
          and pk.table_name = upper(p_source_one)
          and fk.table_name = upper(p_source_many)
          and rownum = 1;
        return v_constraint_name;
    exception
        when no_data_found then
            return null;
    end;

    function build_ddl_for_record(p_table varchar2, p_columns strarr) return varchar2 is
        v_columns_ddl    strarr := strarr();
        v_column_details column_record;
        v_name           varchar2(200);
        v_type           varchar2(200);
    begin
        for i in 1 .. p_columns.count
            loop
                v_column_details := get_column_details(p_table, p_columns(i));

                v_name := 'column' || i;
                if v_column_details.data_type = 'VARCHAR2' THEN
                    v_type := 'VARCHAR2(32767)';
                else
                    v_type := v_column_details.data_type;
                end if;

                v_columns_ddl.extend;
                v_columns_ddl(v_columns_ddl.count) := v_name || ' ' || v_type;
            end loop;
        return 'create or replace type ' || p_source_many || '_rowtype is object(' ||
               join_strarr(v_columns_ddl, ',') || ')';
    end;
begin
    v_fk := get_foreign_key_column(p_source_one, p_source_many);
    if v_fk is null then
        raise_application_error(-20001,
                                'Между таблицей ' || p_source_one || ' и ' || p_source_many ||
                                ' нет отношения один-ко-многим.');
    end if;

    v_columns := get_columns(p_source_many);

    v_record_type_name := p_source_many || '_rowtype';
    v_table_type_name := p_source_many || '_rowtype_t';

    EXECUTE IMMEDIATE 'drop type ' || v_table_type_name;

    v_sql := build_ddl_for_record(p_source_many, v_columns);
    dbms_output.put_line('DDL for record type: ' || v_sql);
    EXECUTE IMMEDIATE v_sql;

    v_sql :=
            'create or replace type ' || v_table_type_name || ' as table of ' || v_record_type_name;
    dbms_output.put_line('DDL for table type: ' || v_sql);
    EXECUTE IMMEDIATE v_sql;

    for i in 1 .. v_columns.count
        loop
            v_columns_prefixed.extend;
            v_columns_prefixed(v_columns_prefixed.count) := 'ch."' || v_columns(i) || '"';
        end loop;

    v_sql := 'select par.*, ' ||
             '(select cast(collect(' || v_record_type_name || '(' ||
             join_strarr(v_columns_prefixed, ',') || ')) as ' || v_table_type_name || ') from ' ||
             p_source_many || ' ch where ch.' || v_fk || ' =  par.id) as associated from ' ||
             p_source_one || ' par';
    dbms_output.put_line('Query: ' || v_sql);

    execute immediate 'create or replace view ' || p_source_one || '_' || p_source_many || ' as ' ||
                      v_sql;
end;

call dump_relation('lw1_client', 'lw1_project');
call dump_relation('lw1_team', 'lw1_project');
select * from lw1_client_lw1_project;
select * from lw1_team_lw1_project;

