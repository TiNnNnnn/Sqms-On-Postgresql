/* A Bison parser, made by GNU Bison 3.7.5.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_BASE_YY_GRAM_H_INCLUDED
# define YY_BASE_YY_GRAM_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int base_yydebug;
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    IDENT = 258,                   /* IDENT  */
    UIDENT = 259,                  /* UIDENT  */
    FCONST = 260,                  /* FCONST  */
    SCONST = 261,                  /* SCONST  */
    USCONST = 262,                 /* USCONST  */
    BCONST = 263,                  /* BCONST  */
    XCONST = 264,                  /* XCONST  */
    Op = 265,                      /* Op  */
    ICONST = 266,                  /* ICONST  */
    PARAM = 267,                   /* PARAM  */
    TYPECAST = 268,                /* TYPECAST  */
    DOT_DOT = 269,                 /* DOT_DOT  */
    COLON_EQUALS = 270,            /* COLON_EQUALS  */
    EQUALS_GREATER = 271,          /* EQUALS_GREATER  */
    LESS_EQUALS = 272,             /* LESS_EQUALS  */
    GREATER_EQUALS = 273,          /* GREATER_EQUALS  */
    NOT_EQUALS = 274,              /* NOT_EQUALS  */
    ABORT_P = 275,                 /* ABORT_P  */
    ABSOLUTE_P = 276,              /* ABSOLUTE_P  */
    ACCESS = 277,                  /* ACCESS  */
    ACTION = 278,                  /* ACTION  */
    ADD_P = 279,                   /* ADD_P  */
    ADMIN = 280,                   /* ADMIN  */
    AFTER = 281,                   /* AFTER  */
    AGGREGATE = 282,               /* AGGREGATE  */
    ALL = 283,                     /* ALL  */
    ALSO = 284,                    /* ALSO  */
    ALTER = 285,                   /* ALTER  */
    ALWAYS = 286,                  /* ALWAYS  */
    ANALYSE = 287,                 /* ANALYSE  */
    ANALYZE = 288,                 /* ANALYZE  */
    AND = 289,                     /* AND  */
    ANY = 290,                     /* ANY  */
    ARRAY = 291,                   /* ARRAY  */
    AS = 292,                      /* AS  */
    ASC = 293,                     /* ASC  */
    ASSERTION = 294,               /* ASSERTION  */
    ASSIGNMENT = 295,              /* ASSIGNMENT  */
    ASYMMETRIC = 296,              /* ASYMMETRIC  */
    AT = 297,                      /* AT  */
    ATTACH = 298,                  /* ATTACH  */
    ATTRIBUTE = 299,               /* ATTRIBUTE  */
    AUTHORIZATION = 300,           /* AUTHORIZATION  */
    BACKWARD = 301,                /* BACKWARD  */
    BEFORE = 302,                  /* BEFORE  */
    BEGIN_P = 303,                 /* BEGIN_P  */
    BETWEEN = 304,                 /* BETWEEN  */
    BIGINT = 305,                  /* BIGINT  */
    BINARY = 306,                  /* BINARY  */
    BIT = 307,                     /* BIT  */
    BOOLEAN_P = 308,               /* BOOLEAN_P  */
    BOTH = 309,                    /* BOTH  */
    BY = 310,                      /* BY  */
    CACHE = 311,                   /* CACHE  */
    CALL = 312,                    /* CALL  */
    CALLED = 313,                  /* CALLED  */
    CASCADE = 314,                 /* CASCADE  */
    CASCADED = 315,                /* CASCADED  */
    CASE = 316,                    /* CASE  */
    CAST = 317,                    /* CAST  */
    CATALOG_P = 318,               /* CATALOG_P  */
    CHAIN = 319,                   /* CHAIN  */
    CHAR_P = 320,                  /* CHAR_P  */
    CHARACTER = 321,               /* CHARACTER  */
    CHARACTERISTICS = 322,         /* CHARACTERISTICS  */
    CHECK = 323,                   /* CHECK  */
    CHECKPOINT = 324,              /* CHECKPOINT  */
    CLASS = 325,                   /* CLASS  */
    CLOSE = 326,                   /* CLOSE  */
    CLUSTER = 327,                 /* CLUSTER  */
    COALESCE = 328,                /* COALESCE  */
    COLLATE = 329,                 /* COLLATE  */
    COLLATION = 330,               /* COLLATION  */
    COLUMN = 331,                  /* COLUMN  */
    COLUMNS = 332,                 /* COLUMNS  */
    COMMENT = 333,                 /* COMMENT  */
    COMMENTS = 334,                /* COMMENTS  */
    COMMIT = 335,                  /* COMMIT  */
    COMMITTED = 336,               /* COMMITTED  */
    CONCURRENTLY = 337,            /* CONCURRENTLY  */
    CONFIGURATION = 338,           /* CONFIGURATION  */
    CONFLICT = 339,                /* CONFLICT  */
    CONNECTION = 340,              /* CONNECTION  */
    CONSTRAINT = 341,              /* CONSTRAINT  */
    CONSTRAINTS = 342,             /* CONSTRAINTS  */
    CONTENT_P = 343,               /* CONTENT_P  */
    CONTINUE_P = 344,              /* CONTINUE_P  */
    CONVERSION_P = 345,            /* CONVERSION_P  */
    COPY = 346,                    /* COPY  */
    COST = 347,                    /* COST  */
    CREATE = 348,                  /* CREATE  */
    CROSS = 349,                   /* CROSS  */
    CSV = 350,                     /* CSV  */
    CUBE = 351,                    /* CUBE  */
    CURRENT_P = 352,               /* CURRENT_P  */
    CURRENT_CATALOG = 353,         /* CURRENT_CATALOG  */
    CURRENT_DATE = 354,            /* CURRENT_DATE  */
    CURRENT_ROLE = 355,            /* CURRENT_ROLE  */
    CURRENT_SCHEMA = 356,          /* CURRENT_SCHEMA  */
    CURRENT_TIME = 357,            /* CURRENT_TIME  */
    CURRENT_TIMESTAMP = 358,       /* CURRENT_TIMESTAMP  */
    CURRENT_USER = 359,            /* CURRENT_USER  */
    CURSOR = 360,                  /* CURSOR  */
    CYCLE = 361,                   /* CYCLE  */
    DATA_P = 362,                  /* DATA_P  */
    DATABASE = 363,                /* DATABASE  */
    DAY_P = 364,                   /* DAY_P  */
    DEALLOCATE = 365,              /* DEALLOCATE  */
    DEC = 366,                     /* DEC  */
    DECIMAL_P = 367,               /* DECIMAL_P  */
    DECLARE = 368,                 /* DECLARE  */
    DEFAULT = 369,                 /* DEFAULT  */
    DEFAULTS = 370,                /* DEFAULTS  */
    DEFERRABLE = 371,              /* DEFERRABLE  */
    DEFERRED = 372,                /* DEFERRED  */
    DEFINER = 373,                 /* DEFINER  */
    DELETE_P = 374,                /* DELETE_P  */
    DELIMITER = 375,               /* DELIMITER  */
    DELIMITERS = 376,              /* DELIMITERS  */
    DEPENDS = 377,                 /* DEPENDS  */
    DESC = 378,                    /* DESC  */
    DETACH = 379,                  /* DETACH  */
    DICTIONARY = 380,              /* DICTIONARY  */
    DISABLE_P = 381,               /* DISABLE_P  */
    DISCARD = 382,                 /* DISCARD  */
    DISTINCT = 383,                /* DISTINCT  */
    DO = 384,                      /* DO  */
    DOCUMENT_P = 385,              /* DOCUMENT_P  */
    DOMAIN_P = 386,                /* DOMAIN_P  */
    DOUBLE_P = 387,                /* DOUBLE_P  */
    DROP = 388,                    /* DROP  */
    EACH = 389,                    /* EACH  */
    ELSE = 390,                    /* ELSE  */
    ENABLE_P = 391,                /* ENABLE_P  */
    ENCODING = 392,                /* ENCODING  */
    ENCRYPTED = 393,               /* ENCRYPTED  */
    END_P = 394,                   /* END_P  */
    ENUM_P = 395,                  /* ENUM_P  */
    ESCAPE = 396,                  /* ESCAPE  */
    EVENT = 397,                   /* EVENT  */
    EXCEPT = 398,                  /* EXCEPT  */
    EXCLUDE = 399,                 /* EXCLUDE  */
    EXCLUDING = 400,               /* EXCLUDING  */
    EXCLUSIVE = 401,               /* EXCLUSIVE  */
    EXECUTE = 402,                 /* EXECUTE  */
    EXISTS = 403,                  /* EXISTS  */
    EXPLAIN = 404,                 /* EXPLAIN  */
    EXPRESSION = 405,              /* EXPRESSION  */
    EXTENSION = 406,               /* EXTENSION  */
    EXTERNAL = 407,                /* EXTERNAL  */
    EXTRACT = 408,                 /* EXTRACT  */
    FALSE_P = 409,                 /* FALSE_P  */
    FAMILY = 410,                  /* FAMILY  */
    FETCH = 411,                   /* FETCH  */
    FILTER = 412,                  /* FILTER  */
    FIRST_P = 413,                 /* FIRST_P  */
    FLOAT_P = 414,                 /* FLOAT_P  */
    FOLLOWING = 415,               /* FOLLOWING  */
    FOR = 416,                     /* FOR  */
    FORCE = 417,                   /* FORCE  */
    FOREIGN = 418,                 /* FOREIGN  */
    FORWARD = 419,                 /* FORWARD  */
    FREEZE = 420,                  /* FREEZE  */
    FROM = 421,                    /* FROM  */
    FULL = 422,                    /* FULL  */
    FUNCTION = 423,                /* FUNCTION  */
    FUNCTIONS = 424,               /* FUNCTIONS  */
    GENERATED = 425,               /* GENERATED  */
    GLOBAL = 426,                  /* GLOBAL  */
    GRANT = 427,                   /* GRANT  */
    GRANTED = 428,                 /* GRANTED  */
    GREATEST = 429,                /* GREATEST  */
    GROUP_P = 430,                 /* GROUP_P  */
    GROUPING = 431,                /* GROUPING  */
    GROUPS = 432,                  /* GROUPS  */
    HANDLER = 433,                 /* HANDLER  */
    HAVING = 434,                  /* HAVING  */
    HEADER_P = 435,                /* HEADER_P  */
    HOLD = 436,                    /* HOLD  */
    HOUR_P = 437,                  /* HOUR_P  */
    IDENTITY_P = 438,              /* IDENTITY_P  */
    IF_P = 439,                    /* IF_P  */
    ILIKE = 440,                   /* ILIKE  */
    IMMEDIATE = 441,               /* IMMEDIATE  */
    IMMUTABLE = 442,               /* IMMUTABLE  */
    IMPLICIT_P = 443,              /* IMPLICIT_P  */
    IMPORT_P = 444,                /* IMPORT_P  */
    IN_P = 445,                    /* IN_P  */
    INCLUDE = 446,                 /* INCLUDE  */
    INCLUDING = 447,               /* INCLUDING  */
    INCREMENT = 448,               /* INCREMENT  */
    INDEX = 449,                   /* INDEX  */
    INDEXES = 450,                 /* INDEXES  */
    INHERIT = 451,                 /* INHERIT  */
    INHERITS = 452,                /* INHERITS  */
    INITIALLY = 453,               /* INITIALLY  */
    INLINE_P = 454,                /* INLINE_P  */
    INNER_P = 455,                 /* INNER_P  */
    INOUT = 456,                   /* INOUT  */
    INPUT_P = 457,                 /* INPUT_P  */
    INSENSITIVE = 458,             /* INSENSITIVE  */
    INSERT = 459,                  /* INSERT  */
    INSTEAD = 460,                 /* INSTEAD  */
    INT_P = 461,                   /* INT_P  */
    INTEGER = 462,                 /* INTEGER  */
    INTERSECT = 463,               /* INTERSECT  */
    INTERVAL = 464,                /* INTERVAL  */
    INTO = 465,                    /* INTO  */
    INVOKER = 466,                 /* INVOKER  */
    IS = 467,                      /* IS  */
    ISNULL = 468,                  /* ISNULL  */
    ISOLATION = 469,               /* ISOLATION  */
    JOIN = 470,                    /* JOIN  */
    KEY = 471,                     /* KEY  */
    LABEL = 472,                   /* LABEL  */
    LANGUAGE = 473,                /* LANGUAGE  */
    LARGE_P = 474,                 /* LARGE_P  */
    LAST_P = 475,                  /* LAST_P  */
    LATERAL_P = 476,               /* LATERAL_P  */
    LEADING = 477,                 /* LEADING  */
    LEAKPROOF = 478,               /* LEAKPROOF  */
    LEAST = 479,                   /* LEAST  */
    LEFT = 480,                    /* LEFT  */
    LEVEL = 481,                   /* LEVEL  */
    LIKE = 482,                    /* LIKE  */
    LIMIT = 483,                   /* LIMIT  */
    LISTEN = 484,                  /* LISTEN  */
    LOAD = 485,                    /* LOAD  */
    LOCAL = 486,                   /* LOCAL  */
    LOCALTIME = 487,               /* LOCALTIME  */
    LOCALTIMESTAMP = 488,          /* LOCALTIMESTAMP  */
    LOCATION = 489,                /* LOCATION  */
    LOCK_P = 490,                  /* LOCK_P  */
    LOCKED = 491,                  /* LOCKED  */
    LOGGED = 492,                  /* LOGGED  */
    MAPPING = 493,                 /* MAPPING  */
    MATCH = 494,                   /* MATCH  */
    MATERIALIZED = 495,            /* MATERIALIZED  */
    MAXVALUE = 496,                /* MAXVALUE  */
    METHOD = 497,                  /* METHOD  */
    MINUTE_P = 498,                /* MINUTE_P  */
    MINVALUE = 499,                /* MINVALUE  */
    MODE = 500,                    /* MODE  */
    MONTH_P = 501,                 /* MONTH_P  */
    MOVE = 502,                    /* MOVE  */
    NAME_P = 503,                  /* NAME_P  */
    NAMES = 504,                   /* NAMES  */
    NATIONAL = 505,                /* NATIONAL  */
    NATURAL = 506,                 /* NATURAL  */
    NCHAR = 507,                   /* NCHAR  */
    NEW = 508,                     /* NEW  */
    NEXT = 509,                    /* NEXT  */
    NFC = 510,                     /* NFC  */
    NFD = 511,                     /* NFD  */
    NFKC = 512,                    /* NFKC  */
    NFKD = 513,                    /* NFKD  */
    NO = 514,                      /* NO  */
    NONE = 515,                    /* NONE  */
    NORMALIZE = 516,               /* NORMALIZE  */
    NORMALIZED = 517,              /* NORMALIZED  */
    NOT = 518,                     /* NOT  */
    NOTHING = 519,                 /* NOTHING  */
    NOTIFY = 520,                  /* NOTIFY  */
    NOTNULL = 521,                 /* NOTNULL  */
    NOWAIT = 522,                  /* NOWAIT  */
    NULL_P = 523,                  /* NULL_P  */
    NULLIF = 524,                  /* NULLIF  */
    NULLS_P = 525,                 /* NULLS_P  */
    NUMERIC = 526,                 /* NUMERIC  */
    OBJECT_P = 527,                /* OBJECT_P  */
    OF = 528,                      /* OF  */
    OFF = 529,                     /* OFF  */
    OFFSET = 530,                  /* OFFSET  */
    OIDS = 531,                    /* OIDS  */
    OLD = 532,                     /* OLD  */
    ON = 533,                      /* ON  */
    ONLY = 534,                    /* ONLY  */
    OPERATOR = 535,                /* OPERATOR  */
    OPTION = 536,                  /* OPTION  */
    OPTIONS = 537,                 /* OPTIONS  */
    OR = 538,                      /* OR  */
    ORDER = 539,                   /* ORDER  */
    ORDINALITY = 540,              /* ORDINALITY  */
    OTHERS = 541,                  /* OTHERS  */
    OUT_P = 542,                   /* OUT_P  */
    OUTER_P = 543,                 /* OUTER_P  */
    OVER = 544,                    /* OVER  */
    OVERLAPS = 545,                /* OVERLAPS  */
    OVERLAY = 546,                 /* OVERLAY  */
    OVERRIDING = 547,              /* OVERRIDING  */
    OWNED = 548,                   /* OWNED  */
    OWNER = 549,                   /* OWNER  */
    PARALLEL = 550,                /* PARALLEL  */
    PARSER = 551,                  /* PARSER  */
    PARTIAL = 552,                 /* PARTIAL  */
    PARTITION = 553,               /* PARTITION  */
    PASSING = 554,                 /* PASSING  */
    PASSWORD = 555,                /* PASSWORD  */
    PLACING = 556,                 /* PLACING  */
    PLANS = 557,                   /* PLANS  */
    POLICY = 558,                  /* POLICY  */
    POSITION = 559,                /* POSITION  */
    PRECEDING = 560,               /* PRECEDING  */
    PRECISION = 561,               /* PRECISION  */
    PRESERVE = 562,                /* PRESERVE  */
    PREPARE = 563,                 /* PREPARE  */
    PREPARED = 564,                /* PREPARED  */
    PRIMARY = 565,                 /* PRIMARY  */
    PRIOR = 566,                   /* PRIOR  */
    PRIVILEGES = 567,              /* PRIVILEGES  */
    PROCEDURAL = 568,              /* PROCEDURAL  */
    PROCEDURE = 569,               /* PROCEDURE  */
    PROCEDURES = 570,              /* PROCEDURES  */
    PROGRAM = 571,                 /* PROGRAM  */
    PUBLICATION = 572,             /* PUBLICATION  */
    QUOTE = 573,                   /* QUOTE  */
    RANGE = 574,                   /* RANGE  */
    READ = 575,                    /* READ  */
    REAL = 576,                    /* REAL  */
    REASSIGN = 577,                /* REASSIGN  */
    RECHECK = 578,                 /* RECHECK  */
    RECURSIVE = 579,               /* RECURSIVE  */
    REF_P = 580,                   /* REF_P  */
    REFERENCES = 581,              /* REFERENCES  */
    REFERENCING = 582,             /* REFERENCING  */
    REFRESH = 583,                 /* REFRESH  */
    REINDEX = 584,                 /* REINDEX  */
    RELATIVE_P = 585,              /* RELATIVE_P  */
    RELEASE = 586,                 /* RELEASE  */
    RENAME = 587,                  /* RENAME  */
    REPEATABLE = 588,              /* REPEATABLE  */
    REPLACE = 589,                 /* REPLACE  */
    REPLICA = 590,                 /* REPLICA  */
    RESET = 591,                   /* RESET  */
    RESTART = 592,                 /* RESTART  */
    RESTRICT = 593,                /* RESTRICT  */
    RETURNING = 594,               /* RETURNING  */
    RETURNS = 595,                 /* RETURNS  */
    REVOKE = 596,                  /* REVOKE  */
    RIGHT = 597,                   /* RIGHT  */
    ROLE = 598,                    /* ROLE  */
    ROLLBACK = 599,                /* ROLLBACK  */
    ROLLUP = 600,                  /* ROLLUP  */
    ROUTINE = 601,                 /* ROUTINE  */
    ROUTINES = 602,                /* ROUTINES  */
    ROW = 603,                     /* ROW  */
    ROWS = 604,                    /* ROWS  */
    RULE = 605,                    /* RULE  */
    SAVEPOINT = 606,               /* SAVEPOINT  */
    SCHEMA = 607,                  /* SCHEMA  */
    SCHEMAS = 608,                 /* SCHEMAS  */
    SCROLL = 609,                  /* SCROLL  */
    SEARCH = 610,                  /* SEARCH  */
    SECOND_P = 611,                /* SECOND_P  */
    SECURITY = 612,                /* SECURITY  */
    SELECT = 613,                  /* SELECT  */
    SEQUENCE = 614,                /* SEQUENCE  */
    SEQUENCES = 615,               /* SEQUENCES  */
    SERIALIZABLE = 616,            /* SERIALIZABLE  */
    SERVER = 617,                  /* SERVER  */
    SESSION = 618,                 /* SESSION  */
    SESSION_USER = 619,            /* SESSION_USER  */
    SET = 620,                     /* SET  */
    SETS = 621,                    /* SETS  */
    SETOF = 622,                   /* SETOF  */
    SHARE = 623,                   /* SHARE  */
    SHOW = 624,                    /* SHOW  */
    SIMILAR = 625,                 /* SIMILAR  */
    SIMPLE = 626,                  /* SIMPLE  */
    SKIP = 627,                    /* SKIP  */
    SMALLINT = 628,                /* SMALLINT  */
    SNAPSHOT = 629,                /* SNAPSHOT  */
    SOME = 630,                    /* SOME  */
    SQL_P = 631,                   /* SQL_P  */
    STABLE = 632,                  /* STABLE  */
    STANDALONE_P = 633,            /* STANDALONE_P  */
    START = 634,                   /* START  */
    STATEMENT = 635,               /* STATEMENT  */
    STATISTICS = 636,              /* STATISTICS  */
    STDIN = 637,                   /* STDIN  */
    STDOUT = 638,                  /* STDOUT  */
    STORAGE = 639,                 /* STORAGE  */
    STORED = 640,                  /* STORED  */
    STRICT_P = 641,                /* STRICT_P  */
    STRIP_P = 642,                 /* STRIP_P  */
    SUBSCRIPTION = 643,            /* SUBSCRIPTION  */
    SUBSTRING = 644,               /* SUBSTRING  */
    SUPPORT = 645,                 /* SUPPORT  */
    SYMMETRIC = 646,               /* SYMMETRIC  */
    SYSID = 647,                   /* SYSID  */
    SYSTEM_P = 648,                /* SYSTEM_P  */
    TABLE = 649,                   /* TABLE  */
    TABLES = 650,                  /* TABLES  */
    TABLESAMPLE = 651,             /* TABLESAMPLE  */
    TABLESPACE = 652,              /* TABLESPACE  */
    TEMP = 653,                    /* TEMP  */
    TEMPLATE = 654,                /* TEMPLATE  */
    TEMPORARY = 655,               /* TEMPORARY  */
    TEXT_P = 656,                  /* TEXT_P  */
    THEN = 657,                    /* THEN  */
    TIES = 658,                    /* TIES  */
    TIME = 659,                    /* TIME  */
    TIMESTAMP = 660,               /* TIMESTAMP  */
    TO = 661,                      /* TO  */
    TRAILING = 662,                /* TRAILING  */
    TRANSACTION = 663,             /* TRANSACTION  */
    TRANSFORM = 664,               /* TRANSFORM  */
    TREAT = 665,                   /* TREAT  */
    TRIGGER = 666,                 /* TRIGGER  */
    TRIM = 667,                    /* TRIM  */
    TRUE_P = 668,                  /* TRUE_P  */
    TRUNCATE = 669,                /* TRUNCATE  */
    TRUSTED = 670,                 /* TRUSTED  */
    TYPE_P = 671,                  /* TYPE_P  */
    TYPES_P = 672,                 /* TYPES_P  */
    UESCAPE = 673,                 /* UESCAPE  */
    UNBOUNDED = 674,               /* UNBOUNDED  */
    UNCOMMITTED = 675,             /* UNCOMMITTED  */
    UNENCRYPTED = 676,             /* UNENCRYPTED  */
    UNION = 677,                   /* UNION  */
    UNIQUE = 678,                  /* UNIQUE  */
    UNKNOWN = 679,                 /* UNKNOWN  */
    UNLISTEN = 680,                /* UNLISTEN  */
    UNLOGGED = 681,                /* UNLOGGED  */
    UNTIL = 682,                   /* UNTIL  */
    UPDATE = 683,                  /* UPDATE  */
    USER = 684,                    /* USER  */
    USING = 685,                   /* USING  */
    VACUUM = 686,                  /* VACUUM  */
    VALID = 687,                   /* VALID  */
    VALIDATE = 688,                /* VALIDATE  */
    VALIDATOR = 689,               /* VALIDATOR  */
    VALUE_P = 690,                 /* VALUE_P  */
    VALUES = 691,                  /* VALUES  */
    VARCHAR = 692,                 /* VARCHAR  */
    VARIADIC = 693,                /* VARIADIC  */
    VARYING = 694,                 /* VARYING  */
    VERBOSE = 695,                 /* VERBOSE  */
    VERSION_P = 696,               /* VERSION_P  */
    VIEW = 697,                    /* VIEW  */
    VIEWS = 698,                   /* VIEWS  */
    VOLATILE = 699,                /* VOLATILE  */
    WHEN = 700,                    /* WHEN  */
    WHERE = 701,                   /* WHERE  */
    WHITESPACE_P = 702,            /* WHITESPACE_P  */
    WINDOW = 703,                  /* WINDOW  */
    WITH = 704,                    /* WITH  */
    WITHIN = 705,                  /* WITHIN  */
    WITHOUT = 706,                 /* WITHOUT  */
    WORK = 707,                    /* WORK  */
    WRAPPER = 708,                 /* WRAPPER  */
    WRITE = 709,                   /* WRITE  */
    XML_P = 710,                   /* XML_P  */
    XMLATTRIBUTES = 711,           /* XMLATTRIBUTES  */
    XMLCONCAT = 712,               /* XMLCONCAT  */
    XMLELEMENT = 713,              /* XMLELEMENT  */
    XMLEXISTS = 714,               /* XMLEXISTS  */
    XMLFOREST = 715,               /* XMLFOREST  */
    XMLNAMESPACES = 716,           /* XMLNAMESPACES  */
    XMLPARSE = 717,                /* XMLPARSE  */
    XMLPI = 718,                   /* XMLPI  */
    XMLROOT = 719,                 /* XMLROOT  */
    XMLSERIALIZE = 720,            /* XMLSERIALIZE  */
    XMLTABLE = 721,                /* XMLTABLE  */
    YEAR_P = 722,                  /* YEAR_P  */
    YES_P = 723,                   /* YES_P  */
    ZONE = 724,                    /* ZONE  */
    NOT_LA = 725,                  /* NOT_LA  */
    NULLS_LA = 726,                /* NULLS_LA  */
    WITH_LA = 727,                 /* WITH_LA  */
    POSTFIXOP = 728,               /* POSTFIXOP  */
    UMINUS = 729                   /* UMINUS  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 211 "gram.y"

	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;

	char				chr;
	bool				boolean;
	JoinType			jtype;
	DropBehavior		dbehavior;
	OnCommitAction		oncommit;
	List				*list;
	Node				*node;
	Value				*value;
	ObjectType			objtype;
	TypeName			*typnam;
	FunctionParameter   *fun_param;
	FunctionParameterMode fun_param_mode;
	ObjectWithArgs		*objwithargs;
	DefElem				*defelt;
	SortBy				*sortby;
	WindowDef			*windef;
	JoinExpr			*jexpr;
	IndexElem			*ielem;
	Alias				*alias;
	RangeVar			*range;
	IntoClause			*into;
	WithClause			*with;
	InferClause			*infer;
	OnConflictClause	*onconflict;
	A_Indices			*aind;
	ResTarget			*target;
	struct PrivTarget	*privtarget;
	AccessPriv			*accesspriv;
	struct ImportQual	*importqual;
	InsertStmt			*istmt;
	VariableSetStmt		*vsetstmt;
	PartitionElem		*partelem;
	PartitionSpec		*partspec;
	PartitionBoundSpec	*partboundspec;
	RoleSpec			*rolespec;
	struct SelectLimit	*selectlimit;

#line 583 "gram.h"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif



int base_yyparse (core_yyscan_t yyscanner);

#endif /* !YY_BASE_YY_GRAM_H_INCLUDED  */
