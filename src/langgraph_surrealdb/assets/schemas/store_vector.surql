(type::is_string("[[vector_table]]") AND "[[vector_table]]".matches("^[a-zA-Z0-9_]+$")) OR
    THROW "[[vector_table]] must be a string and match the regex '^[a-zA-Z0-9_]+$'";

(type::is_string("[[store_table]]") AND "[[store_table]]".matches("^[a-zA-Z0-9_]+$")) OR
    THROW "[[store_table]] must be a string and match the regex '^[a-zA-Z0-9_]+$'";

(type::is_int([[dimensions]]) AND [[dimensions]] > 0) OR
    THROW "[[dimensions]] must be a positive integer";

(type::is_string("[[distance_type]]") AND "[[distance_type]]" IN ["cosine"]) OR
    THROW "[[distance_type]] must be a valid distance type: 'cosine'";


DEFINE TABLE IF NOT EXISTS [[vector_table]] SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD IF NOT EXISTS item ON [[vector_table]] TYPE record<[[store_table]]>;
DEFINE FIELD IF NOT EXISTS namespace ON [[vector_table]] TYPE array<string>;
DEFINE FIELD IF NOT EXISTS key ON [[vector_table]] TYPE string;
DEFINE FIELD IF NOT EXISTS field_name ON [[vector_table]] TYPE string;
DEFINE FIELD IF NOT EXISTS indexed_text ON [[vector_table]] TYPE string;
DEFINE FIELD IF NOT EXISTS embedding ON [[vector_table]] TYPE array<float>;
DEFINE FIELD IF NOT EXISTS created_at ON [[vector_table]] TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS updated_at ON [[vector_table]] TYPE datetime DEFAULT time::now();

DEFINE INDEX OVERWRITE [[vector_table]]_embedding_idx ON [[vector_table]]
    FIELDS embedding HNSW DIMENSION [[dimensions]] DIST [[distance_type]];

DEFINE FUNCTION OVERWRITE fn::remove_english_stopwords($input: string) -> string {
    LET $stopwords = [
        "a", "an", "and", "are", "for", "how", "in", "is", "of", "on", "or",
        "the", "to", "what", "where", "which", "who", "why", "with"
    ];
    RETURN string::split(string::lowercase($input), " ")
        .filter(|$token| {
            return $token != "" AND $token NOT IN $stopwords;
        })
        .join(" ");
};

DEFINE ANALYZER OVERWRITE english_prose_analyzer
    FUNCTION fn::remove_english_stopwords
    TOKENIZERS blank, class, camel, punct
    FILTERS lowercase, ascii, snowball(english);

DEFINE INDEX OVERWRITE [[vector_table]]_text_fts_idx ON [[vector_table]]
    FIELDS indexed_text
    FULLTEXT ANALYZER english_prose_analyzer BM25(1.2, 0.75);


# Search queries

DEFINE FUNCTION OVERWRITE fn::langgraph::store::search_fts(
    $namespace: array<string>,
    $query: string,
    $limit: int,
    $filters: option<object>,
) -> array<{
    id: record<[[store_table]]>,
    score: float
}>
{
    LET $MATCHES = (
        SELECT
            item AS id,
            search::score(0) AS score
        FROM [[vector_table]]
        WHERE
            fn::langgraph::store::cmp_namespace(namespace, $namespace) AND
            fn::langgraph::store::apply_filters(item.value, $filters) AND
            indexed_text @0,OR@ $query
        ORDER BY score DESC
        LIMIT $limit * 3
    );

    RETURN (
        SELECT
            id,
            math::max(score) AS score
        FROM $MATCHES
        GROUP BY id
        ORDER BY score DESC
        LIMIT $limit
    );
};

DEFINE FUNCTION OVERWRITE fn::langgraph::store::search_vec(
    $namespace: array<string>,
    $embedding: array<float>,
    $limit: int,
    $filters: option<object>,
) -> array<{
    id: record<[[store_table]]>,
    score: float
}>
{
    LET $MATCHES = (
        SELECT
            item as id,
            vector::distance::knn() as distance
        FROM [[vector_table]]
        WHERE
            fn::langgraph::store::cmp_namespace(namespace, $namespace) AND
            fn::langgraph::store::apply_filters(item.value, $filters) AND
            embedding <|50,200|> $embedding
        ORDER BY distance ASC
    );

    LET $DEDUPED = (
        SELECT
            id,
            math::min(distance) AS distance
        FROM $MATCHES
        GROUP BY id
        ORDER BY distance ASC
        LIMIT $limit
    );

    RETURN $DEDUPED.map(|$r| {
        RETURN {
            id: $r.id,
            score: 1.0 - $r.distance,
        };
    });

};


DEFINE FUNCTION OVERWRITE fn::langgraph::store::search_indexed(
    $namespace: array<string>,
    $limit: option<int>,
    $offset: option<int>,
    $filters: option<object>,
    $query: option<string>,
    $embedding: option<array<float>>,
) -> array<{
    item: {
        id: record<[[store_table]]>,
        namespace: array<string>,
        key: string,
        value: object,
        created_at: datetime,
        updated_at: datetime,
        expires_at: datetime | NONE,
        ttl_minutes: float | NONE,
    },
    score: float
}>
{
    if $embedding IS NONE AND $query IS NONE {
        RETURN fn::langgraph::store::search($namespace, $limit, $offset, $filters)
        .map(|$r| {
            return { item: $r, score: 0.0 };
        });
    };

    LET $_offset = $offset ?? 0;
    LET $_limit = $limit ?? 10;
    LET $_total = $_offset + $_limit;
    LET $_search_limit = math::min([50, 2 * $_total]);


    IF $embedding IS NONE {
        LET $FTS = fn::langgraph::store::search_fts($namespace, $query, $_search_limit, $filters);
        RETURN $FTS.slice($_offset, $_limit).map(|$r| {
            RETURN {
                item: fn::langgraph::store::get_item($r.id),
                score: $r.score,
            };
        });
    }
    ELSE IF $query IS NONE {
      LET $VEC = fn::langgraph::store::search_vec($namespace, $embedding, $_search_limit, $filters);
      RETURN $VEC.slice($_offset, $_limit).map(|$r| {
            RETURN {
                item: fn::langgraph::store::get_item($r.id),
                score: $r.score,
            };
        });
    } ELSE {
        LET $VEC = fn::langgraph::store::search_vec($namespace, $embedding, $_search_limit, $filters);
        LET $FTS = fn::langgraph::store::search_fts($namespace, $query, $_search_limit, $filters);
        RETURN search::rrf(
            [$VEC, $FTS],
            $_search_limit
        ).slice($_offset, $_limit).map(|$r| {
            RETURN {
                item: fn::langgraph::store::get_item($r.id),
                score: $r.rrf_score,
            };
        });
    }
};


# Metadata

DEFINE PARAM OVERWRITE $__langgraph__store VALUE object::extend($__langgraph__store, {
    index_enabled: true
});
