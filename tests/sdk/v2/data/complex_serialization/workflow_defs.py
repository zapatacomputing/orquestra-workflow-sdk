################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import typing as t

import orquestra.sdk as sdk

# - Task definitions - #


@sdk.task
def make_a_string():
    """
    Simplest possible task. Takes no arguments, returns a hardcoded value.
    """
    return "hello"


@sdk.task
def capitalize(text: str):
    """
    Simple task that takes an argument.
    """
    return text.capitalize()


@sdk.task
def join_strings(texts: t.List[str]):
    """
    Example task that accepts a container.
    """
    return "".join(texts)


@sdk.task
def get_object_id(obj: object):
    return id(obj)


@sdk.task
def get_objects_id(objs: t.List[object]) -> t.List:
    return [id(obj) for obj in objs]


@sdk.task
def invoke_callable(fn: t.Callable):
    return fn()


@sdk.task
def invoke_callables(fns: t.List[t.Callable]) -> t.List[t.Any]:
    return [fn() for fn in fns]


@sdk.task
def generate_simple_callable(num: int = 1) -> t.Callable:
    def _inner():
        return 42 + num

    return _inner


@sdk.task
def generate_simple_callables(num: int = 1) -> t.List[t.Callable]:
    def _inner_1():
        return 42 + num

    def _inner_2():
        return 43 + num

    # We want the task to have one output. It's content will be a list.
    callables = [_inner_1, _inner_2]

    return callables


class HasNum(t.Protocol):
    num: int


@sdk.task
def get_object_num(obj: HasNum) -> int:
    return obj.num


@sdk.task
def generate_object_with_num(num: int):
    class ObjWithNum(object):
        def __init__(self, num: int = 0):
            # An object with a number
            self.num = num

    return ObjWithNum(num)


@sdk.task
def sum_tuple_numbers(numbers: tuple):
    return sum(numbers)


# - End Tasks definitions - #

# - Workflow definitions - #


@sdk.workflow
def wf_pass_tuple():
    two_nums = (1, 2)
    return [sum_tuple_numbers(two_nums)]


@sdk.workflow
def wf_object_id():
    obj = object()
    return [get_object_id(obj)]


@sdk.workflow
def wf_objects_id():
    n_objects = 2
    objs = [object() for _ in range(n_objects)]
    return [get_objects_id(objs)]


@sdk.workflow
def wf_pass_callable():
    num = 1

    def simple_callable(num):
        def _inner():
            return 42 + num

        return _inner

    fn1 = simple_callable(num)
    return [invoke_callable(fn1)]


@sdk.workflow
def wf_pass_callable_from_task():
    num = 1
    fn1 = generate_simple_callable(num)
    return [invoke_callable(fn1)]


@sdk.workflow
def wf_pass_callables():
    num = 1

    def simple_callable(num):
        def _inner():
            return 42 + num

        return _inner

    fns = [simple_callable(num), simple_callable(num + 1)]
    return [invoke_callables(fns)]


@sdk.workflow
def wf_pass_callables_from_task():
    num = 1
    fns = generate_simple_callables(num)
    return [invoke_callables(fns)]


@sdk.workflow
def wf_pass_obj_with_num():
    num = 1

    class ObjWithNum(object):
        def __init__(self, num: int = 0):
            # An object with a number
            self.num = num

    my_test_obj = ObjWithNum(num)
    return [get_object_num(my_test_obj)]


@sdk.workflow
def wf_pass_obj_with_num_from_task():
    num = 1
    my_test_obj = generate_object_with_num(num)
    return [get_object_num(my_test_obj)]


@sdk.workflow
def wf_pass_callable_from_task_inside_cont_should_fail():
    num = 1
    fn1 = generate_simple_callable(num)
    return [invoke_callables([fn1])]


@sdk.workflow
def wf_futures_in_container_should_fail():
    text1 = make_a_string()
    text2 = make_a_string()

    # texts is a container with futures
    texts = [text1, text2]

    joined = join_strings(texts)

    return [joined]


class NonPicklable:
    def __reduce__(self):
        raise TypeError()


@sdk.workflow
def wf_non_picklable_constant_should_fail():
    """
    Rare case where an object can't be serialized using pickle.
    """
    const: t.Any = NonPicklable()
    return [capitalize(const)]


# - End Workflow definitions - #
