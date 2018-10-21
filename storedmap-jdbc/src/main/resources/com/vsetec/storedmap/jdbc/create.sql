create table @{indexName} (
id varchar(200) primary key, 
val blob,
ind clob
);

create table @{indexName}_lock (
id varchar(200) primary key
);

create table @{indexName}_tags (
id varchar(200),
tag varchar(200),
primary key (tag, id)
);

create table @{indexName}_sort (
id varchar(200) primary key,
sort varchar(200) for bit data
);

create index @{indexName}_sort_ind 
on @{indexName}_sort (sort)
