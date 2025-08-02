CREATE FUNCTION vector_in(cstring) RETURNS vector
   AS '/home/hyh/PGDev/postgresql-12-4/src/tutorial/vector'
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION vector_out(vector) RETURNS cstring
   AS '/home/hyh/PGDev/postgresql-12-4/src/tutorial/vector'
   LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE vector (
   internallength = variable,
   input = vector_in,
   output = vector_out
);

CREATE FUNCTION vector_dimension(vector) RETURNS integer
   AS '/home/hyh/PGDev/postgresql-12-4/src/tutorial/vector'
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION vector_distance(vector, vector) RETURNS float4
   AS '/home/hyh/PGDev/postgresql-12-4/src/tutorial/vector'
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION vector_add(vector, vector) RETURNS vector
   AS '/home/hyh/PGDev/postgresql-12-4/src/tutorial/vector'
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION vector_sub(vector, vector) RETURNS vector
   AS '/home/hyh/PGDev/postgresql-12-4/src/tutorial/vector'
   LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR <#> (
   leftarg = vector,
   procedure = vector_dimension
);

CREATE OPERATOR <-> (
   leftarg = vector,
   rightarg = vector,
   procedure = vector_distance,
   commutator = <->
);

CREATE OPERATOR + (
   leftarg = vector,
   rightarg = vector,
   procedure = vector_add,
   commutator = +
);

CREATE OPERATOR - (
   leftarg = vector,
   rightarg = vector,
   procedure = vector_sub
);
