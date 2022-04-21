Lock Demo
---------

Demo of usage PostgreSQL [advisory locks](https://vladmihalcea.com/how-do-postgresql-advisory-locks-work/).

1. Run Postgres server

    ```sh
    $ docker run -d --name my-postgres -p 55432:5432 \
            -e POSTGRES_PASSWORD=mysecretpassword postgres:12.5-alpine
    ```

2. Connect to Postgres server via command line interface:

    ```sh
    $ docker exec -it my-postgres psql -U postgres
    ```

    And run the following sql:

    ```sql
    CREATE ROLE testuser WITH 
        LOGIN
        PASSWORD 'P@ssw0rd1+';
        
    CREATE DATABASE testdb
        WITH OWNER testuser;
    ```

3. Connect to Postgres server again:

    ```sh
    $ docker exec -it my-postgres psql -d testdb -U testuser 
    ```
    
    And run the following sql:
    
    ```sql
    CREATE TABLE messages (
        id uuid NOT NULL,
        message text NOT NULL,
        status text NOT NULL,
        version int NOT NULL,
        CONSTRAINT messages_pk PRIMARY KEY (id)
    );
    ```

4. Build application using Maven:

    ```she
    $ mvn package
    ```
   
5. Run multiple instances of Consumer:

    ```sh
    $ ./run-consumer.sh
    ```

6. Run Producer:

    ```sh
    $ ./run-producer.sh
    ```
