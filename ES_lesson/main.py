import json

from search_GIGA import Search
# from search_all_MiniLM import Search
import pandas as pd

es = Search()

kb_df = pd.read_csv("../kb.csv", encoding='utf-8')
print("Encode")
kb_df_test = kb_df[:10]

kb_df_test.to_json("kb.json", orient='records', force_ascii=False)

es.reindex()

questions = [
    'Как обновить бизнес-процесс?',
    'записи HAR и Console.log',
    'Как добавить в рассылку "Необоснованная доработка"'
]

results = []
for question in questions:
    ### поиск по совпадениям с помощью ES по полям problem, solution, refs
    print(question)
    results_by_query = es.search(

        query={
            'multi_match': {
                'query': question,
                'fields': ['problem', 'solution', 'refs'],
            }
        },

    )

    for hit in results_by_query["hits"]["hits"]:
        results.append(hit)

    ###Поиск по knn
    result_by_knn = es.search(knn={
        'field': 'embedding',
        'query_vector': es.get_embedding(question),
        'num_candidates': 3,
        'k': 3,
    }
    )

    for hit in result_by_knn["hits"]["hits"]:
        results.append(hit)

    final_results = {}
    ###Ранкирование
    for result in results:
        id = result['_id']
        rank = result['_score']
        if id in final_results:
            final_results[id] += (1 / (1 + rank))
        else:
            final_results[id] = (1 / (1 + rank))

    sorted_final_results = sorted(final_results.items(), key=lambda x: x[1], reverse=True)
    print(sorted_final_results)
    for res in sorted_final_results:
        print(es.retrieve_document(res[0]))

