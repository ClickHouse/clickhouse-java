select
    JSONExtractRaw(abcedfg.fields, 'someDateField___e') as abc_someDateField___e, 
    some_word as sw_someWord,
    JSONExtractString(abcedfg.fields, 'field') as abc_field,
    some_more_words as sw_moreWords ,
    last_word as sw_lastWord,
    JSONExtractInt(abcedfg.fields, 'countOfWords') as abc_countOfWords,
    abcedfg.id as abc_id,
    JSONExtractString(abcedfg.fields, 'somePlace') as abc_somePlace,
    JSONExtractString(abcedfg.fields, 'place') as abc_place,
    JSONExtractInt(abcedfg.fields, 'countOfPlaces') as abc_countOfPlaces,
    abcedfg.name as abc_name,
    (some_more_words * 100 / (even_more_words * (? / 28))) - 100  as sw_wordsPercentChange,
    some_unique_words  as sw_uniqueWords
from (
    select
        abcedfg_id,
        sum(if(toDate(sample_date) >= toDate(?, 'UTC'), 1, 0)) some_more_words,
        count(distinct if(toDate(sample_date) >= toDate(?, 'UTC'), wrd.word_id, null)) some_unique_words,
        sum(if(toDate(sample_date) < toDate(?, 'UTC'), 1, 0)) even_more_words,
        min(toDate(sample_date, 'UTC')) some_word,
        max(toDate(sample_date, 'UTC')) last_word
    from a1234_test.sample wrd
    join a1234_test.abcedfg_list_item itm on itm.abcedfg_id = wrd.abcedfg_id
    where toDate(sample_date, 'UTC') between
         addDays(toDate(?, 'UTC'), -28)
         and toDate(?, 'UTC')
    and wrd.sample_type_id IN (?) 
    and itm.abcedfg_list_id IN (?) 
    and 1 
    group by abcedfg_id
) as wrd
join a1234_test.abcedfg abc on abc.id = wrd.abcedfg_id
order by sw_moreWords desc
 limit ? offset ? 
FORMAT CSVWithNames
