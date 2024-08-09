from typing import List, Dict
from itertools import groupby


def doc_post_processor(docs: List[Dict], func_name: str) -> List[Dict]:
    def aggregate_docs(docs, key_name):
        sorted_docs = sorted(docs, key=lambda x: x["doc"]["well"]["wsn"])
        grouped = groupby(sorted_docs, key=lambda x: x["doc"]["well"]["wsn"])

        output_docs = []
        for _, group in grouped:
            group_list = list(group)
            first_doc = group_list[0]
            output_doc = {
                "id": first_doc["id"],
                "well_id": first_doc["well_id"],
                "repo_id": first_doc["repo_id"],
                "repo_name": first_doc["repo_name"],
                "suite": first_doc["suite"],
                "tag": first_doc["tag"],
                "doc": {
                    key_name: [doc["doc"][key_name] for doc in group_list],
                    "well": first_doc["doc"]["well"],
                },
            }
            output_docs.append(output_doc)
        return output_docs

    aggregation_functions = {
        "aggregate_fmtest": lambda: aggregate_docs(docs, "fmtest"),
        "aggregate_pdtest": lambda: aggregate_docs(docs, "pdtest"),
        "aggregate_perfs": lambda: aggregate_docs(docs, "perfs"),
    }

    if func_name in aggregation_functions:
        return aggregation_functions[func_name]()
    else:
        print("no matching post-processing function:", func_name)
        return []
