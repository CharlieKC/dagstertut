from dagster import graph, op, job


@op
def return_one(context) -> int:
    return 1


@op
def add_one(context, number: int) -> int:
    return number + 1


@graph
def linear():
    add_one(add_one(add_one(return_one())))


@job
def graphjob():
    linear()