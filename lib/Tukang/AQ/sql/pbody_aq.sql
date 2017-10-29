create or replace package body rambutan.pkg_aq as
-- $Id: pbody_aq.sql,v 1.2 2008/03/28 13:13:26 cvs Exp $
-- Queue selection 

type selected_queue_type is table of rambutan.aq_daemon.queue%TYPE index by varchar2(256);

g_daemon_queue_for   selected_queue_type;
g_default_queue_for  selected_queue_type;

-- sets queue for current daemon
procedure set_daemon_queue_for(
    -- queue table to set the queue for
    in_queue_table in rambutan.aq_listener.queue_table%TYPE,
    in_queue       in rambutan.aq_daemon.queue%TYPE 
)
is 
begin
    g_daemon_queue_for(in_queue_table) := in_queue;
end set_daemon_queue_for; 

-- sets current queue for a queue table
procedure set_queue_for(
    -- queue table to set the queue for
    in_queue_table in rambutan.aq_listener.queue_table%TYPE,
    in_queue       in rambutan.aq_daemon.queue%TYPE 
)
is 
begin
    g_default_queue_for(in_queue_table) := in_queue;
end set_queue_for; 

function select_queue_from_(
    -- queue table to select the queue from
    in_queue_table in rambutan.aq_listener.queue_table%TYPE,
    -- suggested name of the queue - uses when exists
    in_queue in rambutan.aq_daemon.queue%TYPE default null,
    -- if queue is not in queue table then current daemon queue (if any) is returned
    -- otherwise default queue is returned (if any)
    -- otherwise first NORMAL_QUEUE is returned
    in_prefer_daemon in boolean default false
)
return rambutan.aq_daemon.queue%TYPE
is
    l_queue_table_name  sys.all_queues.queue_table%TYPE;
    l_queue_owner       sys.all_queues.owner%TYPE;
    -- unqualified name
    l_queue_name        sys.all_queues.name%TYPE;
    -- qualified name
    l_queue             rambutan.aq_daemon.queue%TYPE;
begin
    l_queue_owner := substr(in_queue_table, 1, instr(in_queue_table, '.') - 1 );
    l_queue_table_name := substr(in_queue_table, instr(in_queue_table, '.') + 1);

    -- default queue is supplied - it is searched for
    if in_queue is not null then
        begin
            l_queue_name := substr(in_queue, instr(in_queue, '.') + 1);

            select owner || '.' || name
            into l_queue
            from sys.all_queues
            where owner = l_queue_owner  
                and queue_table = l_queue_table_name
                and name = l_queue_name;
        
            return l_queue; 
        exception when NO_DATA_FOUND then
            -- no data is ignored the queue will be found later
            null;
        end;
    end if;

    if in_prefer_daemon then 
        if g_daemon_queue_for.EXISTS(in_queue_table) then
            return g_daemon_queue_for(in_queue_table);
        end if;
    end if;

    if g_default_queue_for.EXISTS(in_queue_table) then
        return g_default_queue_for(in_queue_table);
    end if;

    -- i will select first ordinary queue
    select queue 
    into l_queue 
    from (
        select owner || '.' || name as queue
        from sys.all_queues
        where owner = l_queue_owner
            and queue_table = l_queue_table_name
            and queue_type = 'NORMAL_QUEUE'
        order by qid asc)
     where rownum <= 1;

     g_default_queue_for(in_queue_table) := l_queue;
     return l_queue;
end select_queue_from_;

function select_queue_from(
    -- queue table to select the queue from
    in_queue_table in rambutan.aq_listener.queue_table%TYPE,
    -- suggested name of the queue - uses when exists
    in_queue in rambutan.aq_daemon.queue%TYPE default null)
return rambutan.aq_daemon.queue%TYPE
is
begin
    return select_queue_from_(in_queue_table, in_queue, false);
end select_queue_from;

function select_daemon_queue_from(
    -- queue table to select the queue from
    in_queue_table in rambutan.aq_listener.queue_table%TYPE,
    -- suggested name of the queue - uses when exists
    in_queue in rambutan.aq_daemon.queue%TYPE default null)
return rambutan.aq_daemon.queue%TYPE
is
begin
    return select_queue_from_(in_queue_table, in_queue, true);
end select_daemon_queue_from;

end pkg_aq;
/
show errors

grant execute on rambutan.pkg_aq to public;



