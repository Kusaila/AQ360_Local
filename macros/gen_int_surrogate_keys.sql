{%- macro gen_int_surrogate_keys(this,hash_key_col_name,int_key_col_name) -%}
    {% 
        set int_table = this.schema + '.int_keys_' + this.table
    %}

    -- Create table to store int keys for hash keys if it doesn't exist
    if object_id ('{{ int_table }}', 'U') is null
    begin
        create table {{ int_table }} (
            int_key int identity(1,1) not null,
            hash_key varchar(4000)
        )
        {% set idx_name = 'int_keys_' + this.table + '__index_on_hash_key' %}
        create nonclustered index {{ idx_name }}
            on {{ int_table }} (hash_key)
    end;

    -- Merge new hash keys that are not in int_table yet
    with hash_key_data as (
        select
            {{ hash_key_col_name }} as hash_key
        from {{ this }}
    )
    merge {{ int_table }} target_tbl
    using hash_key_data src_tbl
        on target_tbl.hash_key = src_tbl.hash_key
    when not matched by target
    then insert (hash_key) values (src_tbl.hash_key);

    -- Update orig table's int_key column with int keys
    update 
        {{ this }}
    set 
        {{ this }}.{{ int_key_col_name }} = int_key.int_key 
    from {{ int_table }} int_key
    where 
        {{ this }}.{{ hash_key_col_name }} = int_key.hash_key
{%- endmacro -%}