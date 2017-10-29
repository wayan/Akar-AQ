create or replace package rambutan.pkg_aq_controler
is 
subtype t_qualified_name is varchar2(64);
procedure select_queue_for(
    in_queue_table t_qualified_name,
    in_queue_name t_qualified_name
);

function selected_queue_for(
    in_queue_table t_qualified_name)
return t_qualified_name;
end pkg_aq_controler;
/
show errors

create or replace package body rambutan.pkg_aq_controler
is
type t_xx is table of t_qualified_name;

g_queue_table t_xx := t_xx();
g_selected_queue t_xx := t_xx();
procedure select_queue_for(
    in_queue_table t_qualified_name,
    in_queue_name t_qualified_name
)
is 
begin
    if g_queue_table.count > 0 then
        for i in g_queue_table.first .. g_queue_table.last    
        loop
            if g_queue_table(i) = in_queue_table then
                g_selected_queue(i) := in_queue_name;
                return;
            end if; 
        end loop;
    end if;
    g_queue_table.extend;
    g_selected_queue.extend;
    g_queue_table(g_queue_table.last) := in_queue_table;
    g_selected_queue(g_queue_table.last) := in_queue_name;
end select_queue_for;

function selected_queue_for(
    in_queue_table t_qualified_name)
return t_qualified_name
is 
    -- unqualified name of the queue table
    l_queue_table_uname varchar2(64); 
    -- owner of the queue 
    l_owner              varchar2(32); 
    l_dot_pos            pls_integer;
    l_queue_name         t_qualified_name;
begin
    -- looking for selected queue
    if g_queue_table.count > 0 then
        for i in g_queue_table.first .. g_queue_table.last    
        loop
            if g_queue_table(i) = in_queue_table then
                return g_selected_queue(i);
            end if; 
        end loop;
    end if;

    -- if not found the first queue (with the lowest qid) for queue table is selected 
    l_dot_pos := instr(in_queue_table, '.');
    l_owner   := substr(in_queue_table, 1, l_dot_pos);
    l_queue_table_uname := substr(in_queue_table, l_dot_pos);
    
    select name 
    into l_queue_name
    from ( 
        select name
        from sys.all_queues
        where owner = l_owner
            and name = l_queue_table_uname
            and queue_type = 'NORMAL QUEUE'
        order by qid
    )
    where rownum <= 1;

    return l_queue_name;
end selected_queue_for;

end pkg_aq_controler;
/
show errors
