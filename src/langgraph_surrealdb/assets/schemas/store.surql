
(type::is_string("[[store_table]]") AND "[[store_table]]".matches("^[a-zA-Z0-9_]+$")) OR
    THROW "[[store_table]] must be a string and match the regex '^[a-zA-Z0-9_]+$'";

# Base table

DEFINE TABLE IF NOT EXISTS [[store_table]] SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD IF NOT EXISTS namespace ON [[store_table]] TYPE array<string>;
DEFINE FIELD IF NOT EXISTS key ON [[store_table]] TYPE string;
DEFINE FIELD IF NOT EXISTS value ON [[store_table]] TYPE object FLEXIBLE;
DEFINE FIELD IF NOT EXISTS created_at ON [[store_table]] TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS updated_at ON [[store_table]] TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS expires_at ON [[store_table]] TYPE option<datetime>;
DEFINE FIELD IF NOT EXISTS ttl_minutes ON [[store_table]] TYPE option<float>;

# Indexes

DEFINE INDEX IF NOT EXISTS [[store_table]]_namespace_key_idx ON [[store_table]]
    FIELDS namespace, key UNIQUE;
DEFINE INDEX IF NOT EXISTS [[store_table]]_expiry_idx ON [[store_table]]
    FIELDS expires_at;


DEFINE FUNCTION OVERWRITE fn::langgraph::store::cmp_namespace($namespace: array<string>, $value: array<string>) -> bool {
    RETURN $namespace.slice(0, $value.len()) = $value;
};

DEFINE FUNCTION OVERWRITE fn::langgraph::store::apply_filter(
    $filter: any,
    $value: any
) -> bool {
    IF type::is_object($filter) {
        LET $keys = object::keys($filter);
        # Any "$" key makes this an operator expression.
        LET $op_keys = $keys.filter(|$key| $key.starts_with("$"));
        IF $op_keys.len() > 0 {
            $keys.len() == 1
                OR THROW "Operator filter must contain exactly one key";
            LET $op = $op_keys.at(0);
            LET $cmp = $filter[$op];
            $op IN ["$eq", "$ne", "$gt", "$gte", "$lt", "$lte"]
                OR THROW "Invalid operator: " + $op;
            RETURN IF $op == "$eq" {
                $value == $cmp
            } ELSE IF $op == "$ne" {
                $value != $cmp
            } ELSE IF $op == "$gt" {
                $value > $cmp
            } ELSE IF $op == "$gte" {
                $value >= $cmp
            } ELSE IF $op == "$lt" {
                $value < $cmp
            } ELSE {
                $value <= $cmp
            };
        };
        # Ordinary objects use recursive partial matching.
        IF !type::is_object($value) {
            RETURN false;
        };
        FOR $key IN $keys {
            IF !fn::langgraph::store::apply_filter(
                $filter[$key],
                $value[$key]
            ) {
                RETURN false;
            };
        };
        RETURN true;
    };
    # Scalars, NULL, and arrays use exact equality.
    RETURN $value == $filter;
};

DEFINE FUNCTION OVERWRITE fn::langgraph::store::apply_filters($item: object, $filters: option<object>) -> bool {
    if $filters is NONE {
        return true
    }
    else {
        FOR $key in object::keys($filters) {
            IF !fn::langgraph::store::apply_filter($filters[$key], $item[$key]) {
                RETURN false
            }
        }
    };
    RETURN true
};

DEFINE FUNCTION OVERWRITE fn::langgraph::store::get_item($id: record<[[store_table]]>) -> {
    id: record<[[store_table]]>,
    namespace: array<string>,
    key: string,
    value: object,
    created_at: datetime,
    updated_at: datetime,
    expires_at: datetime | NONE,
    ttl_minutes: float | NONE,
} | NONE {
    if type::is_none($id) {
        RETURN NONE;
    } else {
        RETURN {
            id: $id,
            namespace: $id.namespace,
            key: $id.key,
            value: $id.value,
            created_at: $id.created_at,
            updated_at: $id.updated_at,
            expires_at: $id.expires_at,
            ttl_minutes: $id.ttl_minutes,
        };
    }
};

DEFINE FUNCTION OVERWRITE fn::langgraph::store::search(
    $namespace: array<string>,
    $limit: option<int>,
    $offset: option<int>,
    $filters: option<object>,
) -> array<{
    id: record<[[store_table]]>,
    namespace: array<string>,
    key: string,
    value: object,
    created_at: datetime,
    updated_at: datetime,
    expires_at: datetime | NONE,
    ttl_minutes: float | NONE,
}>
{
    LET $_offset = $offset ?? 0;
    LET $_limit = $limit ?? 10;
    RETURN (
        SELECT
            VALUE fn::langgraph::store::get_item(id)
        FROM [[store_table]]
        WHERE
            fn::langgraph::store::cmp_namespace(namespace, $namespace) AND
            fn::langgraph::store::apply_filters(value, $filters)
        LIMIT $_limit START $_offset
    );
};

# Metadata

DEFINE PARAM IF NOT EXISTS $__langgraph__store VALUE {
    installed_at: time::now(),
    index_enabled: false
};
DEFINE FUNCTION OVERWRITE fn::langgraph::store::is_installed() -> bool {
    RETURN type::is_datetime($__langgraph__store.installed_at);
};
DEFINE FUNCTION OVERWRITE fn::langgraph::store::is_index_enabled() -> bool {
    RETURN type::bool($__langgraph__store.index_enabled OR false);
};
