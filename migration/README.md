Migrations
==========

Spiffy Migrations are a *very* basic suite of helper functions and process methods to organize migrations. Similar to core spiffy, migrations do no replace actual SQL but simply help you organize sql statements into coherent processes. 

##Design / API Organization

1) Suite : a suite is a collection of processes.
2) Process : a process is one or more operations
3) Operation : an operation is a set of statements gated by an existence check. The built in existince primitives include `column`, `table`, `constraint` and `index`. 

##Code Sample

```golang

import (
    m "github.com/blendlabs/spiffy/migration"
)

func main() {
    createTable := m.Operation(m.CreateTable, m.Statement(`
        CREATE TABLE new_table (
            id serial not null,
            type_id int not null,
            name varchar(255) not null,
            score float not null,
            
        );
    `), "new_table")
    
    createPrimaryKey := m.Operation(m.CreateConstraint, m.Statement(`
        ALTER TABLE new_table ADD CONSTRAINT pk_new_table_id PRIMARY KEY (id);
    `), "pk_new_table_id")
    
    createForeignKey := m.Operation(m.CreateConstraint, m.Statement(`
        ALTER TABLE new_table ADD CONSTRAINT fk_new_table_type_id FOREIGN KEY (type_id) REFERENCES new_type(id);
    `), "fk_new_table_type_id")

    m.Suite(m.New(
        createTable,
        createPrimaryKey,
        createForeignKey,
    )).Run(spiffy.DefaultDb())
}
```