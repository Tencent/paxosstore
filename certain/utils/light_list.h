#pragma once

// An implemetation of light list.

#define LIGHTLIST(ElementType) \
  struct {                     \
    ElementType* first;        \
    ElementType* last;         \
  }

#define LIGHTLIST_ENTRY(ElementType) \
  struct {                           \
    ElementType* next;               \
    ElementType* prev;               \
  }

#define LIGHTLIST_INIT(llist)                  \
  do {                                         \
    *(void**)&(llist)->first = (void*)(llist); \
    *(void**)&(llist)->last = (void*)(llist);  \
  } while (0)

#define LIGHTLIST_ENTRY_INIT(elt, field) \
  do {                                   \
    (elt)->field.next = NULL;            \
    (elt)->field.prev = NULL;            \
  } while (0)

#define ENTRY_IN_LIGHTLIST(elt, field) \
  ((elt)->field.next != NULL && (elt)->field.prev != NULL)

#define LIGHTLIST_INSERT_HEAD(llist, elt, field)  \
  do {                                            \
    (elt)->field.next = (llist)->first;           \
    *(void**)&(elt)->field.prev = (void*)(llist); \
    if ((void*)(llist)->last == (void*)(llist))   \
      (llist)->last = (elt);                      \
    else                                          \
      (llist)->first->field.prev = (elt);         \
    (llist)->first = (elt);                       \
  } while (0)

#define LIGHTLIST_INSERT_TAIL(llist, elt, field)  \
  do {                                            \
    *(void**)&(elt)->field.next = (void*)(llist); \
    (elt)->field.prev = (llist)->last;            \
    if ((void*)(llist)->first == (void*)(llist))  \
      (llist)->first = (elt);                     \
    else                                          \
      (llist)->last->field.next = (elt);          \
    (llist)->last = (elt);                        \
  } while (0)

#define LIGHTLIST_REMOVE(llist, elt, field)              \
  do {                                                   \
    if ((void*)(elt)->field.next == (void*)(llist))      \
      (llist)->last = (elt)->field.prev;                 \
    else                                                 \
      (elt)->field.next->field.prev = (elt)->field.prev; \
    if ((void*)(elt)->field.prev == (void*)(llist))      \
      (llist)->first = (elt)->field.next;                \
    else                                                 \
      (elt)->field.prev->field.next = (elt)->field.next; \
    (elt)->field.next = NULL;                            \
    (elt)->field.prev = NULL;                            \
  } while (0)

#define LIGHTLIST_EMPTY(llist) ((void*)(llist)->first == (void*)(llist))
#define LIGHTLIST_FIRST(llist) ((llist)->first)
#define LIGHTLIST_LAST(llist) ((llist)->last)
