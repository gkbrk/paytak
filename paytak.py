import random
import collections
import threading
import enum

__all__ = [
    "all",
    "debug_dump",
    "execute_dummy",
    "execute_threadpool",
    "immediate",
    "wrap",
]

_empty = type("", (), {})


def _is_dep_value(value):
    attrs = ["debug_name", "dependencies", "value", "done"]

    for attr in attrs:
        if hasattr(value, attr):
            return True


def _get_debug_name(value):
    name = getattr(value, "debug_name", "<dependant value>")
    assert isinstance(name, str)
    return name


def _get_dependencies(value):
    deps = getattr(value, "dependencies", ())
    return deps


def _is_done(value):
    done = getattr(value, "done", False)
    assert isinstance(done, bool)
    return done


def _is_ready(value):
    if _is_done(value):
        return True

    for dep in _get_dependencies(value):
        if not _is_done(dep):
            return False
    return True


def _get_value(value):
    return getattr(value, "value", None)

def _get_retry_count(value):
    return getattr(value, "retry_count", 1)


def _resolve_value(value):
    nop = lambda: None
    fn = getattr(value, "resolve", None) or getattr(value, "fn", None) or nop

    result = None

    for _ in range(_get_retry_count(value)):
        try:
            result = fn()
        except:
            continue
        else:
            break
    else:
        print(f"!!! [ERROR] Could not resolve value after {_get_retry_count(value)} retries")

    value.value = result
    value.done = True
    value.ready = True


def immediate(value):
    dep_val = _empty()
    dep_val.value = value
    dep_val.ready = True
    dep_val.done = True
    dep_val.debug_name = f"<immediate value: {value}>"
    dep_val.resolve = lambda: value
    return dep_val


def debug_dump(value, indent=0):
    l = f"{' ' * indent}{_get_debug_name(value)}"
    print(l)

    for dep in _get_dependencies(value):
        debug_dump(dep, indent + 2)


def all(values):
    dep_val = _empty()
    dep_val.debug_name = f"all({len(values)} values)"

    dep_val.dependencies = []

    for val in values:
        if not _is_dep_value(val):
            val = immediate(val)
        dep_val.dependencies.append(val)

    dep_val.resolve = lambda: [x.value for x in dep_val.dependencies]
    return dep_val


def wrap(fn):
    fn_arg_names = fn.__code__.co_varnames[: fn.__code__.co_argcount]

    def inner(*args):
        dep_val = _empty()
        dep_val.debug_name = f"{fn.__name__}({','.join(fn_arg_names)})"

        dep_val.args = []
        for arg in args:
            if not _is_dep_value(arg):
                arg = immediate(arg)
            dep_val.args.append(arg)
        dep_val.dependencies = dep_val.args

        dep_val.resolve = lambda: fn(*[x.value for x in dep_val.args])
        return dep_val

    return inner


def _recursive_deps(value):
    for dep in _get_dependencies(value):
        yield dep
        yield from _recursive_deps(dep)


def execute_dummy(value):
    values = set(_recursive_deps(value))
    values.add(value)
    done = set()

    while not _is_done(value):
        pending = values - done
        ready = list(filter(lambda x: _is_ready(x), pending))
        val = random.choice(ready)
        _resolve_value(val)
        done.add(val)

    return _get_value(value)


# Thread Pool


class RandomThreadPool:
    def __init__(self, thread_count):
        self._threads = []
        self._tasks = []
        self.exit = False
        self.semaphore = threading.Semaphore(0)
        self.results = collections.deque()
        self.result_semaphore = threading.Semaphore(0)

        for _ in range(thread_count):
            self._threads.append(threading.Thread(target=self.thread_fn))

        for t in self._threads:
            t.start()

    def wait_for_one(self):
        status = self.result_semaphore.acquire(timeout=5)
        if not status:
            return None
        return self.results.popleft()

    def thread_fn(self):
        while not self.exit:
            status = self.semaphore.acquire(timeout=5)
            if not status:
                continue
            task = self._tasks.pop()
            fn, args, kwargs = task
            self.results.append(fn(*args, **kwargs))
            self.result_semaphore.release()

    def submit(self, fn, *args, **kwargs):
        self._tasks.append((fn, args, kwargs))
        self.semaphore.release()

    def shuffle(self):
        random.shuffle(self._tasks)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.exit = True
        for t in self._threads:
            t.join()


def execute_threadpool(dep_value, thread_count=16):
    with RandomThreadPool(thread_count) as threadpool:
        values = set(_recursive_deps(dep_value))
        values.add(dep_value)
        done = set()
        submitted = set()

        while not _is_done(dep_value):
            pending = values - done
            pending = pending - submitted
            ready = list(filter(lambda x: _is_ready(x), pending))

            for x in ready:
                threadpool.submit(_resolve_value, x)
                submitted.add(x)
            if ready:
                threadpool.shuffle()

            result = threadpool.wait_for_one()
            if result:
                done.add(result)

        return _get_value(dep_value)
