
def explode(clock):
    ident, event = clock
    return (ident, event)

def rebuild(ident, event=None):
    if event == None:
        return (ident, 0)
    else:
        return (ident, event)

def seed():
    return (1,0)

def join(clock_left, clock_right):
    ident_left, event_left = clock_left
    ident_right, event_right = clock_right
    new_ident = sum_ident(ident_left, ident_right)
    new_event = join_event(event_left, event_right)
    return (new_ident, new_event)

def fork(clock):
    ident, event = clock
    ident_left, ident_right = split(ident)
    return((ident_left, event), (ident_right, event))

def event(clock):
    ident , event = clock
    switch_event = fill(ident, event)
    if switch_event == event:
        _, new_event = grow(ident, event)
        return (ident, new_event)
    else:
        return (ident, switch_event)

def leq(clock_left, clock_right):
    _, event_left = clock_left
    _, event_right = clock_right
    return leq_event(event_left, event_right)

def peek(clock):
    _ident, event = clock
    return (0, event)

# comparison
def leq_event(left_event, right_event):
    if istuple(left_event) and istuple(right_event):
        lb, ll, lr = left_event
        rb, rl, rr = right_event
        test_left = leq_event(lift(lb, ll), lift(rb, rl))
        test_right = leq_event(lift(lb, lr), lift(rb, rr))
        return (lb <= rb) and test_left and test_right
    elif istuple(left_event):
        lb, ll, lr = left_event
        test_left = leq_event(lift(lb, ll), right_event)
        test_right = leq_event(lift(lb, lr), right_event)
        return lb <= right_event and test_left and test_right
    elif istuple(right_event):
        rb, _, _ = right_event
        return left_event <= rb
    else:
        return left_event <= right_event


# normal forms
def normal_event(event):
    base, left, right = event
    if isint(left) and left == right:
        new_base = base + left
        return new_base
    else:
        minimum = min(get_base(left), get_base(right))
        new_base = base + minimum
        new_left = drop(minimum, left)
        new_right = drop(minimum, right)
        return (new_base, new_left, new_right)

def normal_ident(ident):
    if istuple(ident):
        left, right = ident
        if left == right and left == 0:
            return 0
        elif left == right and left == 1:
            return 1
        else:
            return ident
    else:
        return ident

# fork/join helpers
def sum_ident(x, y):
    if x == 0:
        return y
    elif y == 0:
        return x
    else:
        x_left, x_right = x
        y_left, y_right = y
        new_left = sum_ident(x_left, y_left)
        new_right = sum_ident(x_right, y_right)
        return normal_ident((new_left, new_right))


def split(ident):
    if ident == 0:
        return (0,0)
    elif ident == 1:
        return ((1,0),(0,1))
    else:
        ident_a, ident_b = ident
        if ident_a == 0:
            sub_a, sub_b = split(ident_b)
            return ((0, sub_a), (0, sub_b))
        elif ident_b == 0:
            sub_c, sub_d = split(ident_a)
            return ((sub_c, 0), (sub_d, 0))
        else:
            return ((ident_a, 0), (0, ident_b))

def join_event(left_event, right_event):
    if istuple(left_event) and istuple(right_event):
        lb, lel, ler = left_event
        rb, rel, rer = right_event
        if lb > rb:
            return join_event(right_event, left_event)
        else:
            delta = rb - lb
            new_left = join_event(lel, lift(delta, rel))
            new_right = join_event(ler, lift(delta, rer))
            return normal_event((lb, new_left, new_right))
    elif isint(left_event) and isint(right_event):
        return max(left_event, right_event)
    elif isint(left_event):
        return join_event((left_event, 0, 0), right_event)
    elif isint(right_event):
        return join_event(left_event, (right_event, 0, 0))

# event helpers
def fill(ident, event):
    if ident == 0:
        return event
    elif ident == 1 and istuple(event):
        return get_height(event)
    elif isint(event):
        return event
    else:
        left, right = ident
        base, event_left, event_right = event
        if left == 1:
            new_event_right = fill(right, event_right)
            delta = max(get_height(event_left), get_base(new_event_right))
            return normal_event((base, delta, new_event_right))
        if right == 1:
            new_event_left = fill(left, event_left)
            delta =  max(get_height(event_right), get_height(new_event_left))
            return normal_event((base, new_event_left, delta))
        else:
            new_event_left = fill(left, event_left)
            new_event_right = fill(right, event_right)
            return normal_event((base, new_event_left, new_event_right))

def grow(ident, event):
    if ident == 1 and isint(event):
        return (0, event + 1)
    elif istuple(ident) and istuple(event):
        ident_left, ident_right = ident
        base, event_left, event_right = event
        if ident_left == 0:
            new_ident, new_event_right = grow(ident_right, event_right)
            return (new_ident + 1, (base, event_left, new_event_right))
        elif ident_right == 0:
            new_ident, new_event_left = grow(ident_left, event_left)
            return (new_ident + 1, (base, new_event_left, event_right))
        else:
            new_ident_left, new_event_left = grow(ident_left, event_left)
            new_ident_right, new_event_right = grow(ident_right, event_right)
            if new_ident_left < new_ident_right:
                return (new_ident_left + 1, (base, new_event_left, event_right))
            else:
                return (new_ident_right + 1, (base, event_left, new_event_right))
    else:
        new_ident, new_event = grow(ident, (event, 0, 0))
        return (new_ident + 1000, new_event)


def get_height(event):
    if istuple(event):
        base, left, right = event
        return base + max(get_height(left), get_height(right))
    else:
        return event

def get_base(event):
    if istuple(event):
        base, _, _ = event
        return base
    else: # event is base
        return event

def lift(ident, event):
    if istuple(event):
        base, left, right = event
        return (ident + base, left, right)
    else:
        return ident + event

def drop(ident, event):
    if istuple(event):
        base, left, right = event
        return (base - ident, left, right)
    else:
        return event - ident

def istuple(unknown):
    return isinstance(unknown, tuple)

def isint(unknown):
    return isinstance(unknown, int)