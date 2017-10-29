--$Id: tbl_aq_queue.sql,v 1.2 2008/03/28 13:13:26 cvs Exp $
create table rambutan.aq_queue (
    queue varchar2(256) not null constraint pk_aq_queue primary key,
    auto_start number(1) default 1 not null
);

grant all on rambutan.aq_queue to public;

comment on table rambutan.aq_queue is 'Additional (Akar) information for queue';
comment on column rambutan.aq_queue.queue is 'Qualified name of the queue';
comment on column rambutan.aq_queue.auto_start is '1 if daemon can be run on it automatically';


