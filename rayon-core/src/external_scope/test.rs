use crate::unwind;
use crate::{external_scope, ExternalScope};
use rand::{Rng, SeedableRng};
use rand_xorshift::XorShiftRng;
use std::iter::once;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::vec;

#[test]
fn scope_empty() {
    external_scope(|_| {});
}

#[test]
fn scope_result() {
    let x = external_scope(|_| 22);
    assert_eq!(x, 22);
}

#[test]
fn scope_two() {
    let counter = &AtomicUsize::new(0);
    external_scope(|s| {
        s.spawn(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
        });
        s.spawn(move |_| {
            counter.fetch_add(10, Ordering::SeqCst);
        });
    });

    let v = counter.load(Ordering::SeqCst);
    assert_eq!(v, 11);
}

#[test]
fn scope_divide_and_conquer() {
    let counter_p = &AtomicUsize::new(0);
    external_scope(|s| s.spawn(move |s| divide_and_conquer(s, counter_p, 1024)));

    let counter_s = &AtomicUsize::new(0);
    divide_and_conquer_seq(&counter_s, 1024);

    let p = counter_p.load(Ordering::SeqCst);
    let s = counter_s.load(Ordering::SeqCst);
    assert_eq!(p, s);
}

fn divide_and_conquer<'scope>(scope: &ExternalScope<'scope>, counter: &'scope AtomicUsize, size: usize) {
    if size > 1 {
        scope.spawn(move |scope| divide_and_conquer(scope, counter, size / 2));
        scope.spawn(move |scope| divide_and_conquer(scope, counter, size / 2));
    } else {
        // count the leaves
        counter.fetch_add(1, Ordering::SeqCst);
    }
}

fn divide_and_conquer_seq(counter: &AtomicUsize, size: usize) {
    if size > 1 {
        divide_and_conquer_seq(counter, size / 2);
        divide_and_conquer_seq(counter, size / 2);
    } else {
        // count the leaves
        counter.fetch_add(1, Ordering::SeqCst);
    }
}

struct Tree<T: Send> {
    value: T,
    children: Vec<Tree<T>>,
}

impl<T: Send> Tree<T> {
    fn iter<'s>(&'s self) -> vec::IntoIter<&'s T> {
        once(&self.value)
            .chain(self.children.iter().flat_map(Tree::iter))
            .collect::<Vec<_>>() // seems like it shouldn't be needed... but prevents overflow
            .into_iter()
    }

    fn update<OP>(&mut self, op: OP)
    where
        OP: Fn(&mut T) + Sync,
        T: Send,
    {
        external_scope(|s| self.update_in_scope(&op, s));
    }

    fn update_in_scope<'scope, OP>(&'scope mut self, op: &'scope OP, scope: &ExternalScope<'scope>)
    where
        OP: Fn(&mut T) + Sync,
    {
        let Tree {
            ref mut value,
            ref mut children,
        } = *self;
        scope.spawn(move |scope| {
            for child in children {
                scope.spawn(move |scope| child.update_in_scope(op, scope));
            }
        });

        op(value);
    }
}

fn random_tree(depth: usize) -> Tree<u32> {
    assert!(depth > 0);
    let mut seed = <XorShiftRng as SeedableRng>::Seed::default();
    (0..).zip(seed.as_mut()).for_each(|(i, x)| *x = i);
    let mut rng = XorShiftRng::from_seed(seed);
    random_tree1(depth, &mut rng)
}

fn random_tree1(depth: usize, rng: &mut XorShiftRng) -> Tree<u32> {
    let children = if depth == 0 {
        vec![]
    } else {
        (0..rng.gen_range(0, 4)) // somewhere between 0 and 3 children at each level
            .map(|_| random_tree1(depth - 1, rng))
            .collect()
    };

    Tree {
        value: rng.gen_range(0, 1_000_000),
        children,
    }
}

#[test]
fn update_tree() {
    let mut tree: Tree<u32> = random_tree(10);
    let values: Vec<u32> = tree.iter().cloned().collect();
    tree.update(|v| *v += 1);
    let new_values: Vec<u32> = tree.iter().cloned().collect();
    assert_eq!(values.len(), new_values.len());
    for (&i, &j) in values.iter().zip(&new_values) {
        assert_eq!(i + 1, j);
    }
}

#[test]
#[should_panic(expected = "Hello, world!")]
fn panic_propagate_scope() {
    external_scope(|_| panic!("Hello, world!"));
}

#[test]
#[should_panic(expected = "Hello, world!")]
fn panic_propagate_spawn() {
    external_scope(|s| s.spawn(|_| panic!("Hello, world!")));
}

#[test]
#[should_panic(expected = "Hello, world!")]
fn panic_propagate_nested_spawn() {
    external_scope(|s| s.spawn(|s| s.spawn(|s| s.spawn(|_| panic!("Hello, world!")))));
}

#[test]
fn panic_propagate_still_execute_1() {
    let mut x = false;
    match unwind::halt_unwinding(|| {
        external_scope(|s| {
            s.spawn(|_| panic!("Hello, world!")); // job A
            s.spawn(|_| x = true); // job B, should still execute even though A panics
        });
    }) {
        Ok(_) => panic!("failed to propagate panic"),
        Err(_) => assert!(x, "job b failed to execute"),
    }
}

#[test]
fn panic_propagate_still_execute_2() {
    let mut x = false;
    match unwind::halt_unwinding(|| {
        external_scope(|s| {
            s.spawn(|_| x = true); // job B, should still execute even though A panics
            s.spawn(|_| panic!("Hello, world!")); // job A
        });
    }) {
        Ok(_) => panic!("failed to propagate panic"),
        Err(_) => assert!(x, "job b failed to execute"),
    }
}

#[test]
fn panic_propagate_still_execute_3() {
    let mut x = false;
    match unwind::halt_unwinding(|| {
        external_scope(|s| {
            s.spawn(|_| x = true); // spawned job should still execute despite later panic
            panic!("Hello, world!");
        });
    }) {
        Ok(_) => panic!("failed to propagate panic"),
        Err(_) => assert!(x, "panic after spawn, spawn failed to execute"),
    }
}

#[test]
fn panic_propagate_still_execute_4() {
    let mut x = false;
    match unwind::halt_unwinding(|| {
        external_scope(|s| {
            s.spawn(|_| panic!("Hello, world!"));
            x = true;
        });
    }) {
        Ok(_) => panic!("failed to propagate panic"),
        Err(_) => assert!(x, "panic in spawn tainted scope"),
    }
}

#[test]
fn non_send_closure() {
    external_scope(|_| {
        Rc::new(1u32)
    });
}

#[test]
fn static_scope() {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    let mut range = 0..100;
    let sum = range.clone().sum();
    let iter = &mut range;

    COUNTER.store(0, Ordering::Relaxed);
    external_scope(|s: &ExternalScope<'static>| {
        // While we're allowed the locally borrowed iterator,
        // the spawns must be static.
        for i in iter {
            s.spawn(move |_| {
                COUNTER.fetch_add(i, Ordering::Relaxed);
            });
        }
    });

    assert_eq!(COUNTER.load(Ordering::Relaxed), sum);
}

#[test]
fn mixed_lifetime_scope() {
    fn increment<'slice, 'counter>(counters: &'slice [&'counter AtomicUsize]) {
        external_scope(move |s: &ExternalScope<'counter>| {
            // We can borrow 'slice here, but the spawns can only borrow 'counter.
            for &c in counters {
                s.spawn(move |_| {
                    c.fetch_add(1, Ordering::Relaxed);
                });
            }
        });
    }

    let counter = AtomicUsize::new(0);
    increment(&[&counter; 100]);
    assert_eq!(counter.into_inner(), 100);
}
