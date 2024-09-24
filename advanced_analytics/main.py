# coding: utf-8

import sys
import traceback

from src import cache_manager, info, expl_task, res_task, session_manager, CustomLogger
from argparse import ArgumentParser


def main():
    logger = CustomLogger("Main")
    parser = ArgumentParser(prog="advanced analytics", usage=__doc__)
    parser.add_argument(
        "--scope",
        type=str,
        required=True,
        choices=["all", "explore", "results"],
        help="Specify which scope to execute: 'all', 'explore', 'results'",
    )
    parser.add_argument(
        "--task",
        type=str,
        choices=[
            "rules",
            "top_pcs",
            "elbow",
            "silhouette",
            "distance_matrix",
            "hierarchical_distance_matrix",
            "dendrogram",
            "archives_frequencies",
            "trend_vector",
        ],
        nargs="+",
        help="Specify which task to execute in exploratory analysis: 'rules', 'top_pcs', 'elbow', 'silhouette', 'distance_matrix', 'hierarchical_clustering', 'dendrogram', 'archives_frequencies', 'trend_vector'",
    )

    args = parser.parse_args()
    if not args.scope:
        logger.warn("No arguments chosen")
        sys.exit(1)
    session_manager.get_session()
    try:
        if args.scope == "all" or args.scope == "explore":
            if not args.task or "rules" in args.task:
                expl_task.association_rules()
            if not args.task or "top_pcs" in args.task:
                expl_task.top_n()
            if not args.task or "elbow" in args.task:
                expl_task.elbow()
            if not args.task or "silhouette" in args.task:
                expl_task.silhouette()
            if not args.task or "distance_matrix" in args.task:
                expl_task.distance_matrix()
            if not args.task or "hierarchical_distance_matrix" in args.task:
                expl_task.hierarchical_distance_matrix()
            if not args.task or "dendrogram" in args.task:
                expl_task.dendrogram()
            expl_task.update_info()
        if args.scope == "all" or args.scope == "results":
            if not args.task or "archives_frequencies" in args.task:
                res_task.archives_frequencies()
            if not args.task or "trend_vector" in args.task:
                res_task.trend_vector()
            res_task.update_info()
        info.save()
    except Exception as e:
        traceback.print_exc()
    finally:
        cache_manager.clear()
        session_manager.stop()


if __name__ == "__main__":
    main()
