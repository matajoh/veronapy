#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef _WIN32
#include <windows.h>
typedef volatile long long atomic_llong;
typedef PVOID voidptr_t;
typedef volatile PVOID atomic_voidptr_t;
typedef volatile LONG atomic_bool;
typedef CONDITION_VARIABLE cnd_t;
typedef CRITICAL_SECTION mtx_t;
typedef HANDLE thrd_t;
typedef int thrd_return_t;
#define thrd_success 0
#define mtx_plain 0
typedef int (*thrd_start_t)(void *);
#define thread_local __declspec(thread)

atomic_llong atomic_increment(atomic_llong *ptr)
{
  return InterlockedIncrement64(ptr);
}

atomic_llong atomic_decrement(atomic_llong *ptr)
{
  return InterlockedDecrement64(ptr);
}

voidptr_t atomic_exchange_ptr(atomic_voidptr_t *ptr, atomic_voidptr_t val)
{
  return InterlockedExchangePointer(ptr, val);
}

bool atomic_compare_exchange_ptr(atomic_voidptr_t *ptr, atomic_voidptr_t *expected, atomic_voidptr_t desired)
{
  atomic_voidptr_t prev;

  prev = InterlockedCompareExchangePointer(ptr, desired, *expected);
  if (prev == *expected)
  {
    return true;
  }

  *expected = prev;
  return false;
}

bool atomic_compare_exchange_bool(atomic_bool *ptr, bool *expected, bool desired)
{
  bool prev;

  prev = InterlockedCompareExchange(ptr, desired, *expected);
  if (prev == *expected)
  {
    return true;
  }

  *expected = prev;
  return false;
}

voidptr_t atomic_load_ptr(atomic_voidptr_t *ptr)
{
  return *ptr;
}

bool atomic_load_bool(atomic_bool *ptr)
{
  return *ptr;
}

void atomic_store_bool(atomic_bool *ptr, bool val)
{
  InterlockedExchange(ptr, val);
}

int mtx_init(mtx_t *mtx, int type)
{
  InitializeCriticalSection(mtx);
  return thrd_success;
}

int mtx_lock(mtx_t *mtx)
{
  EnterCriticalSection(mtx);
  return thrd_success;
}

int mtx_unlock(mtx_t *mtx)
{
  LeaveCriticalSection(mtx);
  return thrd_success;
}

int mtx_destroy(mtx_t *mtx)
{
  DeleteCriticalSection(mtx);
  return thrd_success;
}

int cnd_init(cnd_t *cond)
{
  InitializeConditionVariable(cond);
  return thrd_success;
}

int cnd_destroy(cnd_t *cond)
{
  // Nothing to do
  return thrd_success;
}

int cnd_signal(cnd_t *cond)
{
  WakeConditionVariable(cond);
  return thrd_success;
}

int cnd_broadcast(cnd_t *cond)
{
  WakeAllConditionVariable(cond);
  return thrd_success;
}

int cnd_wait(cnd_t *cond, mtx_t *mtx)
{
  SleepConditionVariableCS(cond, mtx, INFINITE);
  return thrd_success;
}

int thrd_create(thrd_t *thr, thrd_start_t func, void *arg)
{
  *thr = CreateThread(NULL, 0, func, arg, 0, NULL);
  if (*thr == NULL)
  {
    return GetLastError();
  }

  return thrd_success;
}

void thrd_yield()
{
  Sleep(0);
}

int thrd_join(thrd_t thr, int *res)
{
  DWORD rc = WaitForSingleObject(thr, INFINITE);
  if (rc == WAIT_OBJECT_0)
  {
    return thrd_success;
  }

  return GetLastError();
}
#else
#include <stdatomic.h>

typedef intptr_t voidptr_t;
typedef atomic_intptr_t atomic_voidptr_t;

long long atomic_increment(atomic_llong *ptr)
{
  return atomic_fetch_add(ptr, 1) + 1;
}

long long atomic_decrement(atomic_llong *ptr)
{
  return atomic_fetch_sub(ptr, 1) - 1;
}

voidptr_t atomic_load_ptr(atomic_voidptr_t *ptr)
{
  return atomic_load(ptr);
}

bool atomic_load_bool(atomic_bool *ptr)
{
  return atomic_load(ptr);
}

void atomic_store_bool(atomic_bool *ptr, bool val)
{
  atomic_store(ptr, val);
}

voidptr_t atomic_exchange_ptr(atomic_voidptr_t *ptr, voidptr_t val)
{
  return atomic_exchange(ptr, val);
}

bool atomic_compare_exchange_ptr(atomic_voidptr_t *ptr, voidptr_t *expected, voidptr_t desired)
{
  return atomic_compare_exchange_strong(ptr, expected, desired);
}

bool atomic_compare_exchange_bool(atomic_bool *ptr, bool *expected, bool desired)
{
  return atomic_compare_exchange_strong(ptr, expected, desired);
}

#ifdef __APPLE__
#include <pthread.h>

typedef pthread_mutex_t mtx_t;
typedef pthread_cond_t cnd_t;
typedef pthread_t thrd_t;
typedef void *thrd_return_t;
typedef thrd_return_t (*thrd_start_t)(void *);

#define VPY_PTHREAD
#define mtx_plain PTHREAD_MUTEX_NORMAL
#define thrd_success 0

int mtx_init(mtx_t *mtx, int type)
{
  return pthread_mutex_init(mtx, NULL);
}

int mtx_lock(mtx_t *mtx)
{
  return pthread_mutex_lock(mtx);
}

int mtx_unlock(mtx_t *mtx)
{
  return pthread_mutex_unlock(mtx);
}

int mtx_destroy(mtx_t *mtx)
{
  return pthread_mutex_destroy(mtx);
}

int cnd_init(cnd_t *cond)
{
  return pthread_cond_init(cond, NULL);
}

int cnd_destroy(cnd_t *cond)
{
  return pthread_cond_destroy(cond);
}

int cnd_signal(cnd_t *cond)
{
  return pthread_cond_signal(cond);
}

int cnd_broadcast(cnd_t *cond)
{
  return pthread_cond_broadcast(cond);
}

int cnd_wait(cnd_t *cond, mtx_t *mtx)
{
  return pthread_cond_wait(cond, mtx);
}

int thrd_create(thrd_t *thr, thrd_start_t func, void *arg)
{
  return pthread_create(thr, NULL, func, arg);
}

int thrd_yield()
{
  return sleep(0);
}

int thrd_join(thrd_t thr, int *res)
{
  return pthread_join(thr, NULL);
}

#else
#include <threads.h>
typedef int thrd_return_t;
#endif

#endif

/***************************************************************/
/*                  Hashtable Implementation                   */
/***************************************************************/

typedef struct hashtable_entry_s
{
  voidptr_t key;
  voidptr_t value;
} ht_entry;

typedef struct hashtable_s
{
  ht_entry *entries;
  Py_ssize_t capacity;
  Py_ssize_t length;
  bool threadsafe;
  mtx_t mutex;
} ht;

static ht *ht_create(Py_ssize_t capacity, bool threadsafe)
{
  ht *table = (ht *)malloc(sizeof(ht));
  if (table == NULL)
  {
    return NULL;
  }

  table->entries = (ht_entry *)calloc(capacity, sizeof(ht_entry));
  if (table->entries == NULL)
  {
    free(table);
    return NULL;
  }

  table->capacity = capacity;
  table->length = 0;

  if (threadsafe)
  {
    if (mtx_init(&table->mutex, mtx_plain) != thrd_success)
    {
      free(table->entries);
      free(table);
      return NULL;
    }
    table->threadsafe = true;
  }
  else
  {
    table->threadsafe = false;
  }

  return table;
}

static void ht_free(ht *table)
{
  free(table->entries);
  if (table->threadsafe)
  {
    mtx_destroy(&table->mutex);
  }

  free(table);
}

static void ht_lock(ht *table)
{
  if (table->threadsafe)
  {
    mtx_lock(&table->mutex);
  }
}

static void ht_unlock(ht *table)
{
  if (table->threadsafe)
  {
    mtx_unlock(&table->mutex);
  }
}

#if SIZEOF_VOID_P == 8
#define HASH_SHIFT 3
#else
#define HASH_SHIFT 2
#endif

static Py_ssize_t hash_key(voidptr_t key)
{
  return (Py_ssize_t)(key >> HASH_SHIFT);
}

static voidptr_t ht_get(ht *table, voidptr_t key)
{
  ht_lock(table);
  Py_ssize_t hash = hash_key(key);
  Py_ssize_t index = (hash & (table->capacity - 1));

  voidptr_t result = 0;
  while (table->entries[index].key != 0)
  {
    if (table->entries[index].key == key)
    {
      result = table->entries[index].value;
      break;
    }

    index++;
    if (index >= table->capacity)
    {
      index = 0;
    }
  }

  ht_unlock(table);
  return result;
}

static void ht_set_entry(ht *table, voidptr_t key, voidptr_t value)
{
  Py_ssize_t hash = hash_key(key);
  Py_ssize_t index = (hash & (table->capacity - 1));

  while (table->entries[index].key != 0)
  {
    if (table->entries[index].key == key)
    {
      table->entries[index].value = value;
    }

    index++;
    if (index >= table->capacity)
    {
      index = 0;
    }
  }

  table->entries[index].key = key;
  table->entries[index].value = value;
  table->length++;
}

static bool ht_should_expand(Py_ssize_t length, Py_ssize_t capacity)
{
  return length * 2 >= capacity;
}

static bool ht_expand_if_needed(ht *table, Py_ssize_t min_length)
{
  if (!ht_should_expand(min_length, table->capacity))
  {
    return true;
  }

  Py_ssize_t new_capacity = table->capacity * 2;
  while (ht_should_expand(min_length, new_capacity))
  {
    new_capacity = 2 * new_capacity;
  }

  ht *new_table = ht_create(new_capacity, false);
  if (new_table == 0)
  {
    return false;
  }

  for (Py_ssize_t i = 0; i < table->capacity; ++i)
  {
    ht_entry entry = table->entries[i];
    if (entry.key != 0)
    {
      ht_set_entry(new_table, entry.key, entry.value);
    }
  }

  free(table->entries);
  table->entries = new_table->entries;
  table->capacity = new_table->capacity;
  table->length = new_table->length;
  free(new_table);
  return true;
}

static bool ht_set(ht *table, voidptr_t key, voidptr_t value)
{
  bool result = true;
  assert(key != 0);
  if (key == 0)
  {
    return false;
  }

  ht_lock(table);

  if (!ht_expand_if_needed(table, table->length))
  {
    ht_unlock(table);
    return false;
  }

  if (result)
  {
    ht_set_entry(table, key, value);
  }

  ht_unlock(table);

  return result;
}

#if 0
/***************************************************************/
/*                  Linked-List Implementation                 */
/***************************************************************/

typedef struct list_node_s
{
  voidptr_t value;
  struct list_node_s *next;
} list_node;

typedef struct list_s
{
  list_node *head;
  list_node *tail;
  Py_ssize_t length;
} list;

static list *list_create()
{
  list *l = (list *)malloc(sizeof(list));
  if (l == NULL)
  {
    return NULL;
  }

  l->head = NULL;
  l->tail = NULL;
  l->length = 0;
  return l;
}

static void list_free(list *l)
{
  list_node *node = l->head;
  while (node != NULL)
  {
    list_node *next = node->next;
    free(node);
    node = next;
  }

  free(l);
}

static void list_append(list *l, voidptr_t value)
{
  list_node *node = (list_node *)malloc(sizeof(list_node));
  if (node == NULL)
  {
    return;
  }

  node->value = value;
  node->next = NULL;
  if (l->head == NULL)
  {
    l->head = l->tail = node;
  }
  else
  {
    l->tail->next = node;
    l->tail = node;
  }

  l->length++;
}
#endif

/***************************************************************/
/*                     Macros and Defines                      */
/***************************************************************/

#if PY_VERSION_HEX < 0x030C0000 // Python 3.12
#define PyType_GetDict(t) ((t)->tp_dict)
#else
#define VPY_MULTIGIL
#endif

#define VPY_DEBUG

#ifdef VPY_DEBUG
char VPY_DEBUG_FMT_BUF[1024];
#define PRINTDBG print_debug
#else
#define PRINTDBG(...)
#endif

#define _VPY_GETTYPE(n, i, r)                                            \
  n = get_type(i);                                                       \
  if (n == NULL)                                                         \
  {                                                                      \
    PyErr_SetString(PyExc_TypeError,                                     \
                    "error obtaining internal type of isolated object"); \
    return r;                                                            \
  }

#define VPY_GETTYPE(r) \
  PyTypeObject *type;  \
  _VPY_GETTYPE(type, isolated_type, r)

#define _VPY_GETREGION(n, r)                                      \
  RegionTagObject *region_tag;                                    \
  region_tag = get_tag(self, true);                               \
  if (region_tag == NULL)                                         \
  {                                                               \
    PyErr_SetString(PyExc_TypeError,                              \
                    "error obtaining region of isolated object"); \
    return r;                                                     \
  }                                                               \
  n = region_tag->region;

#define VPY_GETREGION(r) \
  RegionObject *region;  \
  _VPY_GETREGION(region, r)

#define VPY_CHECKREGIONOPEN(r)                                   \
  if (!region->is_open)                                          \
  {                                                              \
    PyErr_SetString(RegionIsolationError, "Region is not open"); \
    return r;                                                    \
  }

#define VPY_CHECKARG0REGION(r)                                   \
  PyTypeObject *arg0_type = Py_TYPE(arg0);                       \
  PyTypeObject *arg0_isolated_type = arg0_type;                  \
  RegionObject *arg0_region = region_of(arg0);                   \
  if (arg0_region == NULL)                                       \
  {                                                              \
    arg0_type = arg0_isolated_type;                              \
    capture_object(region, arg0);                                \
    arg0_region = region;                                        \
    arg0_isolated_type = Py_TYPE(arg0);                          \
  }                                                              \
  else if (arg0_region == region)                                \
  {                                                              \
    _VPY_GETTYPE(arg0_type, arg0_isolated_type, r);              \
  }                                                              \
  else                                                           \
  {                                                              \
    PyErr_SetString(RegionIsolationError,                        \
                    "First argument belongs to another region"); \
    return r;                                                    \
  }

#define VPY_CHECKARG1REGION(r)                                    \
  PyTypeObject *arg1_type = Py_TYPE(arg1);                        \
  PyTypeObject *arg1_isolated_type = arg1_type;                   \
  RegionObject *arg1_region = region_of(arg1);                    \
  if (arg1_region == NULL)                                        \
  {                                                               \
    arg1_type = arg1_isolated_type;                               \
    capture_object(region, arg1);                                 \
    arg1_region = region;                                         \
    arg1_isolated_type = Py_TYPE(arg1);                           \
  }                                                               \
  else if (arg1_region == region)                                 \
  {                                                               \
    _VPY_GETTYPE(arg1_type, arg1_isolated_type, r);               \
  }                                                               \
  else                                                            \
  {                                                               \
    PyErr_SetString(RegionIsolationError,                         \
                    "Second argument belongs to another region"); \
    return r;                                                     \
  }

#define VPY_LENFUNC(interface, name)                 \
  Py_ssize_t Isolated_##name(PyObject *self)         \
  {                                                  \
    PyTypeObject *isolated_type = Py_TYPE(self);     \
    VPY_GETTYPE(0);                                  \
    VPY_GETREGION(0);                                \
    VPY_CHECKREGIONOPEN(0);                          \
    self->ob_type = type;                            \
    Py_ssize_t length = type->interface->name(self); \
    self->ob_type = isolated_type;                   \
    return length;                                   \
  }

#define VPY_INQUIRY(interface, name)             \
  int Isolated_##name(PyObject *self)            \
  {                                              \
    PyTypeObject *isolated_type = Py_TYPE(self); \
    VPY_GETTYPE(0);                              \
    VPY_GETREGION(0);                            \
    VPY_CHECKREGIONOPEN(0);                      \
    self->ob_type = type;                        \
    int result = type->interface->name(self);    \
    self->ob_type = isolated_type;               \
    return result;                               \
  }

#define VPY_UNARYFUNC(interface, name)              \
  PyObject *Isolated_##name(PyObject *self)         \
  {                                                 \
    PyTypeObject *isolated_type = Py_TYPE(self);    \
    VPY_GETTYPE(NULL);                              \
    VPY_GETREGION(NULL);                            \
    VPY_CHECKREGIONOPEN(NULL);                      \
    self->ob_type = type;                           \
    PyObject *result = type->interface->name(self); \
    self->ob_type = isolated_type;                  \
    return result;                                  \
  }

#define VPY_BINARYFUNC(interface, name)                     \
  PyObject *Isolated_##name(PyObject *self, PyObject *arg0) \
  {                                                         \
    PyTypeObject *isolated_type = Py_TYPE(self);            \
    VPY_GETTYPE(NULL);                                      \
    VPY_GETREGION(NULL);                                    \
    VPY_CHECKREGIONOPEN(NULL);                              \
    if (arg0 != NULL)                                       \
    {                                                       \
      VPY_CHECKARG0REGION(NULL);                            \
    }                                                       \
    self->ob_type = type;                                   \
    PyObject *result = type->interface->name(self, arg0);   \
    self->ob_type = isolated_type;                          \
    return result;                                          \
  }

#define VPY_TERNARYFUNC(interface, name)                                    \
  PyObject *Isolated_##name(PyObject *self, PyObject *arg0, PyObject *arg1) \
  {                                                                         \
    PyTypeObject *isolated_type = Py_TYPE(self);                            \
    VPY_GETTYPE(NULL);                                                      \
    VPY_GETREGION(NULL);                                                    \
    VPY_CHECKREGIONOPEN(NULL);                                              \
    if (arg0 != NULL)                                                       \
    {                                                                       \
      VPY_CHECKARG0REGION(NULL);                                            \
    }                                                                       \
    if (arg1 != NULL)                                                       \
    {                                                                       \
      VPY_CHECKARG1REGION(NULL);                                            \
    }                                                                       \
    self->ob_type = type;                                                   \
    PyObject *result = type->interface->name(self, arg0, arg1);             \
    self->ob_type = isolated_type;                                          \
    return result;                                                          \
  }

#define VPY_OBJOBJARGPROC(interface, name)                            \
  int Isolated_##name(PyObject *self, PyObject *arg0, PyObject *arg1) \
  {                                                                   \
    PyTypeObject *isolated_type = Py_TYPE(self);                      \
    int rc;                                                           \
    VPY_GETTYPE(-1);                                                  \
    VPY_GETREGION(-1);                                                \
    VPY_CHECKREGIONOPEN(-1);                                          \
    VPY_CHECKARG0REGION(-1);                                          \
    if (arg1 != NULL)                                                 \
    {                                                                 \
      VPY_CHECKARG1REGION(-1);                                        \
    }                                                                 \
    self->ob_type = type;                                             \
    rc = type->interface->name(self, arg0, arg1);                     \
    self->ob_type = isolated_type;                                    \
    return rc;                                                        \
  }

#define VPY_SSIZEARGFUNC(interface, name)                   \
  PyObject *Isolated_##name(PyObject *self, Py_ssize_t arg) \
  {                                                         \
    PyTypeObject *isolated_type = Py_TYPE(self);            \
    VPY_GETTYPE(NULL);                                      \
    VPY_GETREGION(NULL);                                    \
    VPY_CHECKREGIONOPEN(NULL);                              \
    self->ob_type = type;                                   \
    PyObject *result = type->interface->name(self, arg);    \
    self->ob_type = isolated_type;                          \
    return result;                                          \
  }

#define VPY_SSIZEOBJARGPROC(interface, name)                           \
  int Isolated_##name(PyObject *self, Py_ssize_t arg0, PyObject *arg1) \
  {                                                                    \
    PyTypeObject *isolated_type = Py_TYPE(self);                       \
    int rc;                                                            \
    VPY_GETTYPE(-1);                                                   \
    VPY_GETREGION(-1);                                                 \
    VPY_CHECKREGIONOPEN(-1);                                           \
    if (arg1 != NULL)                                                  \
    {                                                                  \
      VPY_CHECKARG1REGION(-1);                                         \
    }                                                                  \
    self->ob_type = type;                                              \
    rc = type->interface->name(self, arg0, arg1);                      \
    self->ob_type = isolated_type;                                     \
    return rc;                                                         \
  }

#define VPY_OBJOBJPROC(interface, name)               \
  int Isolated_##name(PyObject *self, PyObject *arg0) \
  {                                                   \
    PyTypeObject *isolated_type = Py_TYPE(self);      \
    int rc;                                           \
    VPY_GETTYPE(-1);                                  \
    VPY_GETREGION(-1);                                \
    VPY_CHECKREGIONOPEN(-1);                          \
    if (arg0 != NULL)                                 \
    {                                                 \
      VPY_CHECKARG0REGION(-1);                        \
    }                                                 \
    self->ob_type = type;                             \
    rc = type->interface->name(self, arg0);           \
    self->ob_type = isolated_type;                    \
    return rc;                                        \
  }

#define VPY_REGION(x)                   \
  RegionObject *region = (x);           \
  if (region->alias != (PyObject *)(x)) \
  {                                     \
    region = resolve_region(region);    \
  }

#define VPY_ERROR(x) printf("veronapy Error: %s\n", x);

/***************************************************************/
/*                     Useful functions                        */
/***************************************************************/

static thread_local Py_ssize_t alloc_id;

#ifdef VPY_DEBUG
static void print_debug(char *fmt, ...)
{
  char debug_fmt[1024];
  va_list args;

  sprintf(debug_fmt, "%lu: %s", alloc_id, fmt);
  va_start(args, fmt);
  vprintf(debug_fmt, args);
  va_end(args);
}
#endif

static bool is_imm(PyObject *value)
{
  if (Py_IsNone(value) || PyBool_Check(value) || PyLong_Check(value) ||
      PyFloat_Check(value) || PyComplex_Check(value) ||
      PyUnicode_Check(value) || PyBytes_Check(value) || PyRange_Check(value))
  {
    return true;
  }

  if (PyFrozenSet_Check(value) || PyTuple_Check(value))
  {
    Py_ssize_t i, len;
    PyObject *seq = PySequence_List(value);
    if (seq == NULL)
    {
      PyErr_SetString(PyExc_RuntimeError, "Unable to enumerate over sequence");
      return false;
    }

    len = PySequence_Length(seq);
    for (i = 0; i < len; i++)
    {
      PyObject *item = PySequence_GetItem(seq, i);
      if (item == NULL)
      {
        PyErr_SetString(PyExc_RuntimeError, "Unable to get item from sequence");
        return false;
      }

      if (!is_imm(item))
      {
        Py_DECREF(item);
        Py_DECREF(seq);
        return false;
      }

      Py_DECREF(item);
    }

    Py_DECREF(seq);
    return true;
  }

  return false;
}

static PyObject *get_source(PyObject *obj)
{
  PyObject *inspect = PyImport_ImportModule("inspect");
  if (inspect == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to import inspect module");
    return NULL;
  }

  PyObject *getsource = PyObject_GetAttrString(inspect, "getsource");
  if (getsource == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to get getsource from inspect module");
    return NULL;
  }

  PyObject *textwrap = PyImport_ImportModule("textwrap");
  if (textwrap == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to import textwrap module");
    return NULL;
  }

  PyObject *dedent = PyObject_GetAttrString(textwrap, "dedent");
  if (dedent == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to get dedent from textwrap module");
    return NULL;
  }

  PyObject *source = PyObject_CallOneArg(getsource, obj);
  if (source == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to get source of object");
    return NULL;
  }

  source = PyObject_CallOneArg(dedent, source);
  if (source == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to dedent thunk source");
    return NULL;
  }

  return source;
}

/***************************************************************/
/*              Region struct and functions                    */
/***************************************************************/

typedef struct region_object_s
{
  PyObject_HEAD;
  PyObject *name;
  PyObject *alias;
  long long id;
  bool is_open;
  bool is_shared;
  PyObject *parent;
  PyObject *objects;
  atomic_voidptr_t last;
} RegionObject;

typedef struct region_tag_object_s
{
  PyObject_HEAD;
  RegionObject *region;
} RegionTagObject;

typedef struct vpy_state_s
{
  PyObject *isolated_types;
  PyObject *frozen_types;
  PyObject *imm_object_regions;
} VPYState;

static ht *global_imm_object_regions;
static ht *global_frozen_types;
static atomic_llong frozen_type_count = 0;

static thread_local PyObject *RegionIsolationError;
static thread_local PyObject *WhenError;
static thread_local VPYState *vpy_state;

static const char *REGION_ATTRS[] = {
    "name",
    "id",
    "parent",
    "is_open",
    "merge",
    "is_shared",
    "make_shareable",
    "detach_all",
    "__enter__",
    "__exit__",
    "__lt__",
    "__eq__",
    "__hash__",
    NULL,
};

// Whether the given region is free, i.e. has no parent
static bool is_free(RegionObject *region) { return region->parent == NULL; }

// Whether lhs is the root of a tree that contains rhs
static bool owns(RegionObject *lhs, RegionObject *rhs)
{
  if (lhs == rhs)
  {
    return true;
  }

  if (is_free(rhs))
  {
    return false;
  }

  return owns(lhs, (RegionObject *)rhs->parent);
}

// This returns the true region, skipping past aliases.
static RegionObject *resolve_region(RegionObject *start)
{
  RegionObject *region = start;
  while (region->alias != (PyObject *)region)
  {
    PyObject *alias = ((RegionObject *)region->alias)->alias;
    Py_CLEAR(region->alias);
    Py_INCREF(alias);
    region->alias = (PyObject *)alias;

    region = (RegionObject *)region->alias;
  }

  return region;
}

static PyTypeObject *get_type(PyTypeObject *isolated_type)
{
  printf("get_type?\n");
  PRINTDBG("get_type %p\n", isolated_type);
  PyTypeObject *type;
  PyObject *type_id_long = (PyObject *)PyDict_GetItemString(PyType_GetDict(isolated_type), "__isolated__");
  if (type_id_long == NULL)
  {
    PyErr_SetString(PyExc_TypeError,
                    "error obtaining internal type ID of isolated object");
    return NULL;
  }

  if (PyType_Check(type_id_long))
  {
    type = (PyTypeObject *)type_id_long;
    PRINTDBG("get_type frozen type is a built-in or extension type %s\n", type->tp_name);
    return type;
  }

  PyObject *type_tuple = PyDict_GetItem(vpy_state->frozen_types, type_id_long);
  if (type_tuple != NULL)
  {
    type = (PyTypeObject *)PyTuple_GET_ITEM(type_tuple, 0);
    PRINTDBG("type was cached on the interpreter %s\n", type->tp_name);
    return type;
  }

  long long type_id = PyLong_AsLongLong(type_id_long);
  PRINTDBG("Need to load type %li from global table and compile on this interpreter", type_id);

  // need to load this type into the interpreter
  PyObject *source = (PyObject *)ht_get(global_frozen_types, (voidptr_t)type_id);
  if (source == NULL)
  {
    PyErr_SetString(PyExc_TypeError,
                    "error obtaining source code for internal type of isolated object");
    return NULL;
  }

  PyObject *ns = PyDict_New();
  type = (PyTypeObject *)PyRun_String(PyUnicode_AsUTF8(source), Py_file_input, ns, NULL);
  Py_DECREF(ns);
  if (type == NULL)
  {
    PyErr_SetString(PyExc_TypeError,
                    "error loading internal type of isolated object");
    return NULL;
  }

  PRINTDBG("Type loaded %s\n", type->tp_name);

  type_tuple = PyTuple_Pack(2, (PyObject *)type, source);
  if (type_tuple == NULL)
  {
    PyErr_SetString(PyExc_TypeError,
                    "error creating tuple for internal type of isolated object");
    return NULL;
  }

  int rc = PyDict_SetItem(vpy_state->frozen_types, type_id_long, type_tuple);
  if (rc < 0)
  {
    PyErr_SetString(PyExc_TypeError,
                    "error caching internal type of isolated object");
    return NULL;
  }

  return type;
}

static RegionTagObject *get_tag(PyObject *value, bool with_errors)
{
  PyObject *long_key = PyLong_FromVoidPtr(value);
  RegionTagObject *region_tag = (RegionTagObject *)PyDict_GetItem(vpy_state->imm_object_regions, long_key);
  if (region_tag != NULL)
  {
    return region_tag;
  }

  // this could be an immutable type from another interpreter, try the global table
  region_tag = (RegionTagObject *)ht_get(global_imm_object_regions, (voidptr_t)value);
  if (region_tag == NULL)
  {
    if (with_errors)
    {
      PyErr_SetString(PyExc_TypeError, "error obtaining region tag of isolated object");
    }

    return NULL;
  }

  // let's cache this for later
  int rc = PyDict_SetItem((PyObject *)vpy_state->imm_object_regions, long_key, (PyObject *)region_tag);
  if (rc < 0)
  {
    if (with_errors)
    {
      PyErr_SetString(PyExc_TypeError, "error caching region tag of isolated object");
    }

    return NULL;
  }

  return region_tag;
}

// The region of an object (or an implicit region if the object is not
// isolated)
static RegionObject *region_of(PyObject *value)
{
  RegionObject *region;
  RegionTagObject *tag = get_tag(value, false);
  if (tag == NULL)
  {
    return NULL;
  }

  region = tag->region;
  if (region->alias != (PyObject *)region)
  {
    region = resolve_region(region);
    tag->region = region;
  }

  return region;
}

static int capture_object(RegionObject *region, PyObject *value);

// Attempts to capture all the items in a sequence into the given region
static int capture_sequence(RegionObject *region, PyObject *sequence)
{
  PyObject *iterator = PyObject_GetIter(sequence);
  PyObject *item;

  if (iterator == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to get iterator from sequence");
    return -1;
  }

  while ((item = PyIter_Next(iterator)))
  {
    int rc;
    if (item == NULL)
    {
      PyErr_SetString(PyExc_RuntimeError,
                      "Unable to get sequence item from iterator");
      Py_DECREF(iterator);
      return -1;
    }

    rc = capture_object(region, item);
    if (rc != 0)
    {
      Py_DECREF(iterator);
      Py_DECREF(item);
      return rc;
    }

    Py_DECREF(item);
  }

  Py_DECREF(iterator);

  if (PyErr_Occurred())
  {
    return -1;
  }

  return 0;
}

// Attempts to capture all the values in a mapping into the given region
static int capture_mapping(RegionObject *region, PyObject *mapping)
{
  PyObject *values = PyMapping_Values(mapping);
  if (values == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to get values from mapping");
    return -1;
  }

  int rc = capture_sequence(region, values);
  Py_DECREF(values);
  return rc;
}

static PyTypeObject RegionTagType;
static PyTypeObject *isolate_type(PyTypeObject *type);

// Attempts to capture an object into the given region.
// This function will find all the types of all the objects
// in the graph for which value is the root and capture them into the
// region. If they have already been captured, it will use the type it
// previously captured.
static int capture_object(RegionObject *region, PyObject *value)
{
  PyObject *tag;
  PyTypeObject *isolated_type;
  int rc = 0;
  PyTypeObject *type = Py_TYPE(value);
  RegionObject *value_region = region_of(value);

  if (is_imm(value) || value_region == region)
  {
    // this value is either immutable, or already captured by the region.
    return 0;
  }

  if (value_region != NULL)
  {
    // this value is captured by another region
    PyErr_SetString(RegionIsolationError,
                    "Object already captured by another region");
    return -1;
  }

  PRINTDBG("capture_object %p\n", value);

  // ALERT THIS INCREF MAKES THIS OBJECT IMMORTAL
  // Remove once we have region-based memory management
  Py_INCREF(value);
  // ALERT THIS INCREF MAKES THIS OBJECT IMMORTAL

  if (value->ob_type == region->ob_base.ob_type)
  {
    // this is a region object, so we need to add it to our graph
    RegionObject *child = (RegionObject *)value;
    if (is_free(child))
    {
      Py_INCREF(region);
      child->parent = (PyObject *)region;
      return 0;
    }
    else if (!owns(region, child))
    {
      PyErr_SetString(RegionIsolationError,
                      "Region already attached to a different region graph");
      return -1;
    }
  }

  if (PySequence_Check(value))
  {
    rc = capture_sequence(region, value);
    if (rc != 0)
    {
      return rc;
    }
  }
  else if (PyMapping_Check(value))
  {
    rc = capture_mapping(region, value);
    if (rc != 0)
    {
      return rc;
    }
  }

  if (PyObject_HasAttrString(value, "__dict__"))
  {
    PyObject *dict = PyObject_GetAttrString(value, "__dict__");
    PyObject *values = PyDict_Values(dict);
    if (values == NULL)
    {
      PyErr_SetString(PyExc_RuntimeError, "Unable to get values from __dict__");
      return -1;
    }

    rc = capture_sequence(region, values);
    Py_DECREF(values);

    if (rc != 0)
    {
      return rc;
    }
  }

  isolated_type =
      (PyTypeObject *)PyDict_GetItem(vpy_state->isolated_types, (PyObject *)type);
  if (isolated_type == NULL)
  {
    isolated_type = isolate_type(type);
    if (isolated_type == NULL)
    {
      return -1;
    }

    rc = PyDict_SetItem(vpy_state->isolated_types, (PyObject *)type,
                        (PyObject *)isolated_type);
    if (rc < 0)
    {
      PyErr_SetString(RegionIsolationError,
                      "Unable to add isolated type to interpreter");
      return -1;
    }
  }

  tag = PyObject_CallOneArg((PyObject *)&RegionTagType, (PyObject *)region);
  if (tag == NULL)
  {
    PyErr_SetString(RegionIsolationError, "Unable to create region tag");
    return -1;
  }

  Py_INCREF(tag);
  PyObject *long_key = PyLong_FromVoidPtr(value);
  if (long_key == NULL)
  {
    Py_DECREF(tag);
    return -1;
  }
  rc = PyDict_SetItem(vpy_state->imm_object_regions, long_key, tag);
  if (rc < 0)
  {
    PyErr_SetString(RegionIsolationError, "Unable to set region tag");
    Py_DECREF(tag);
    return -1;
  }

  if (!ht_set(global_imm_object_regions, (voidptr_t)value, (voidptr_t)tag))
  {
    PyErr_SetString(RegionIsolationError, "Unable to set region tag in global object table");
    Py_DECREF(tag);
    return -1;
  }

  Py_INCREF((PyObject *)isolated_type);
  value->ob_type = isolated_type;

  return rc;
}

static bool Region_Check(PyObject *obj);

/***************************************************************/
/*                   Behavior Implementation                   */
/***************************************************************/

typedef struct terminator_s
{
  atomic_llong count;
  atomic_bool set;
} Terminator;

typedef struct behavior_s Behavior;

typedef struct request_s
{
  volatile Behavior *next;
  volatile bool scheduled;
  RegionObject *target;
} Request;

typedef struct behavior_s
{
  PyObject *thunk_source;
  PyObject *thunk_locals;
  atomic_llong count;
  Py_ssize_t length;
  Request *requests;
} Behavior;

typedef struct node_s
{
  Behavior *behavior;
  struct node_s *next;
  struct node_s *prev;
} Node;

typedef struct pcqueue_s
{
  Node *front;
  Node *back;
  cnd_t available;
  mtx_t mutex;
  bool active;
} PCQueue;

typedef struct when_object_s
{
  PyObject_HEAD;
  PyObject *regions;
} WhenObject;

typedef struct behavior_exception_s
{
  PyInterpreterState *interp;
  char *name;
  char *msg;
  struct behavior_exception_s *next;
} BehaviorException;

static atomic_voidptr_t behavior_exceptions = (voidptr_t)NULL;
static atomic_llong region_identity = 0;
static atomic_bool running = false;
static Terminator *terminator;
static PCQueue *work_queue;
static Py_ssize_t worker_count = 1;
static thrd_t *workers;
static PyThreadState **subinterpreters;

// NB Unless otherwise specified, none of the Behavior-related functions
// require holding the GIL. Since they are being used from multiple different
// threads this makes many things easier, especially memory management.

static void BehaviorException_new(PyObject *type, PyObject *value, PyObject *traceback)
{
  BehaviorException *ex;
  voidptr_t next;
  const char *str;

  PRINTDBG("BehaviorException_new %p %p %p\n", type, value, traceback);
  ex = (BehaviorException *)malloc(sizeof(BehaviorException));
  if (ex == NULL)
  {
    PRINTDBG("Unable to allocate behavior exception\n");
    goto unraisable;
  }

  if (traceback != NULL)
  {
    if (PyException_SetTraceback(value, traceback) < 0)
    {
      PRINTDBG("Unable to set traceback\n");
      goto unraisable;
    }
  }

  PyObject *nameobj = PyUnicode_FromString(((PyTypeObject *)type)->tp_name);
  if (nameobj == NULL)
  {
    PRINTDBG("Unable to get type name\n");
    goto unraisable;
  }

  str = PyUnicode_AsUTF8(nameobj);
  if (str == NULL)
  {
    PRINTDBG("Unable to get type name\n");
    goto unraisable;
  }

  ex->name = malloc(strlen(str) + 1);
  if (ex->name == NULL)
  {
    PRINTDBG("Unable to allocate type name\n");
    goto unraisable;
  }

  strcpy(ex->name, str);

  Py_DECREF(nameobj);

  if (value != NULL)
  {
    PyObject *msgobj = PyUnicode_FromFormat("%S", value);
    if (msgobj == NULL)
    {
      PRINTDBG("Unable to get exception message\n");
      goto unraisable;
    }

    str = PyUnicode_AsUTF8(msgobj);
    if (str == NULL)
    {
      PRINTDBG("Unable to get exception message\n");
      goto unraisable;
    }

    ex->msg = malloc(strlen(str) + 1);
    if (ex->msg == NULL)
    {
      PRINTDBG("Unable to allocate exception message\n");
      goto unraisable;
    }

    strcpy(ex->msg, str);

    Py_DECREF(msgobj);
  }
  else
  {
    ex->msg = NULL;
  }

  ex->interp = PyInterpreterState_Get();

  next = atomic_exchange_ptr(&behavior_exceptions, (voidptr_t)ex);
  ex->next = (BehaviorException *)next;
  goto end;

unraisable:
  PyErr_WriteUnraisable(value);

end:
  Py_XDECREF(type);
  Py_XDECREF(value);
  Py_XDECREF(traceback);
  return;
}

static void BehaviorException_free(BehaviorException *ex)
{
  if (ex->name != NULL)
  {
    free(ex->name);
  }

  if (ex->msg != NULL)
  {
    free(ex->msg);
  }

  free(ex);
}

static PCQueue *PCQueue_new()
{
  PCQueue *queue;

  PRINTDBG("PCQueue_new\n");

  queue = (PCQueue *)malloc(sizeof(PCQueue));
  if (queue == NULL)
  {
    VPY_ERROR("Unable to allocate queue");
    return NULL;
  }

  queue->front = NULL;
  queue->back = NULL;
  queue->active = true;
  if (cnd_init(&queue->available) != thrd_success)
  {
    VPY_ERROR("Unable to initialize queue condition");
    return NULL;
  }

  if (mtx_init(&queue->mutex, mtx_plain) != thrd_success)
  {
    VPY_ERROR("Unable to initialize queue mutex");
    return NULL;
  }

  return queue;
}

static int PCQueue_enqueue(PCQueue *queue, Behavior *behavior)
{
  PRINTDBG("PCQueue_enqueue\n");
  Node *node = (Node *)malloc(sizeof(Node));
  if (node == NULL)
  {
    VPY_ERROR("Unable to allocate node");
    return -1;
  }

  node->behavior = behavior;
  node->next = NULL;
  node->prev = NULL;
  PRINTDBG("Node created\n");

  PRINTDBG("acquiring queue mutex\n");
  if (mtx_lock(&queue->mutex) != thrd_success)
  {
    VPY_ERROR("Unable to lock queue mutex");
    return -1;
  }

  PRINTDBG("enqueueing node\n");
  if (queue->front == NULL)
  {
    queue->front = node;
    queue->back = node;
  }
  else
  {
    queue->back->next = node;
    node->prev = queue->back;
    queue->back = node;
  }

  PRINTDBG("signalling workers\n");
  if (cnd_signal(&queue->available) != thrd_success)
  {
    VPY_ERROR("Unable to signal queue condition");
    return -1;
  }

  PRINTDBG("unlocking queue mutex\n");
  if (mtx_unlock(&queue->mutex) != thrd_success)
  {
    VPY_ERROR("Unable to unlock queue mutex");
    return -1;
  }

  return 0;
}

static int PCQueue_dequeue(PCQueue *queue, Behavior **behavior)
{
  Node *node;

  *behavior = NULL;
  if (mtx_lock(&queue->mutex) != thrd_success)
  {
    VPY_ERROR("Unable to lock queue mutex");
    return -1;
  }

  while (queue->front == NULL && queue->active)
  {
    if (cnd_wait(&queue->available, &queue->mutex) != thrd_success)
    {
      VPY_ERROR("Unable to wait on queue condition");
      return -1;
    }
  }

  if (!queue->active)
  {
    PRINTDBG("queue is inactive\n");
    if (mtx_unlock(&queue->mutex) != thrd_success)
    {
      VPY_ERROR("Unable to unlock queue mutex");
      return -1;
    }

    return 0;
  }

  node = queue->front;
  queue->front = node->next;
  if (queue->front == NULL)
  {
    queue->back = NULL;
  }
  else
  {
    queue->front->prev = NULL;
  }

  if (mtx_unlock(&queue->mutex) != thrd_success)
  {
    VPY_ERROR("Unable to unlock queue mutex");
    free(node);
    return -1;
  }

  PRINTDBG("dequeued node behavior %p\n", node->behavior);
  *behavior = node->behavior;
  free(node);
  return 0;
}

static int PCQueue_stop(PCQueue *queue)
{
  PRINTDBG("PCQueue_stop\n");

  PRINTDBG("acquiring queue mutex\n");
  if (mtx_lock(&queue->mutex) != thrd_success)
  {
    VPY_ERROR("Unable to lock queue mutex");
    return -1;
  }

  queue->active = false;

  PRINTDBG("broadcasting queue condition\n");

  if (cnd_broadcast(&queue->available) != thrd_success)
  {
    VPY_ERROR("Unable to broadcast queue condition");
    return -1;
  }

  PRINTDBG("unlocking queue mutex\n");

  if (mtx_unlock(&queue->mutex) != thrd_success)
  {
    VPY_ERROR("Unable to unlock queue mutex");
    return -1;
  }

  return 0;
}

static void PCQueue_free(PCQueue *queue)
{
  mtx_destroy(&queue->mutex);
  cnd_destroy(&queue->available);
  free(queue);
}

static Terminator *Terminator_new()
{
  Terminator *terminator;

  PRINTDBG("Terminator_new\n");

  terminator = (Terminator *)malloc(sizeof(Terminator));
  if (terminator == NULL)
  {
    VPY_ERROR("Unable to allocate terminator");
    return NULL;
  }

  terminator->count = 1;
  terminator->set = false;

  return terminator;
}

static void Terminator_free(Terminator *terminator)
{
  free(terminator);
}

static void Terminator_increment(Terminator *terminator)
{
  atomic_increment(&terminator->count);
}

static int Terminator_decrement(Terminator *terminator)
{
  if (atomic_decrement(&terminator->count) == 0LL)
  {
    atomic_store_bool(&terminator->set, true);
  }

  return 0;
}

// GIL must be held
static int Terminator_wait(Terminator *terminator)
{
  int rc;

  PRINTDBG("Terminator_wait\n");

  rc = Terminator_decrement(terminator);
  if (rc != 0)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to decrement terminator");
    return rc;
  }

  while (!atomic_load_bool(&terminator->set))
  {
    Py_BEGIN_ALLOW_THREADS
    thrd_yield();
    Py_END_ALLOW_THREADS
  }

  PRINTDBG("All work complete.\n");

  return 0;
}

static void Request_init(Request *self, RegionObject *region);
// static void Request_free(Request *self);
static int Request_release(Request *self);
static int Request_start_enqueue(Request *self, Behavior *behavior);
static void Request_finish_enqueue(Request *self);

// this must be called while holding the GIL
static Behavior *Behavior_new(PyObject *thunk_source, PyObject *thunk_locals, PyObject *regions)
{
  Py_ssize_t i;
  Request *r;
  Behavior *b = (Behavior *)malloc(sizeof(Behavior));
  if (b == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to allocate behavior");
    return NULL;
  }

  Py_INCREF(thunk_source);
  b->thunk_source = thunk_source;

  Py_INCREF(thunk_locals);
  b->thunk_locals = thunk_locals;

  PyList_Sort(regions);
  b->length = PyList_Size(regions);
  PRINTDBG("Behavior_new %p r#: %li\n", b, b->length);
  b->count = b->length + 1;
  b->requests = (Request *)malloc(sizeof(Request) * b->length);
  if (b->requests == NULL)
  {
    PyMem_FREE(b);
    PyErr_SetString(PyExc_RuntimeError, "Unable to allocate requests");
    return NULL;
  }

  for (i = 0, r = b->requests; i < b->length; ++i, ++r)
  {
    PyObject *region = PyList_GetItem(regions, i);
    if (region == NULL)
    {
      PyMem_FREE(b);
      PyErr_SetString(PyExc_RuntimeError, "Unable to get region from list");
      return NULL;
    }

    Request_init(r, (RegionObject *)region);
  }

  return b;
}

// this must be called while holding the GIL
/* static void Behavior_free(Behavior *self)
{
  Py_ssize_t i;
  Request *r;
  Py_DECREF(self->thunk);
  for (i = 0, r = self->requests; i < self->length; ++i, ++r)
  {
    Request_free(r);
  }

  PyMem_FREE(self->requests);
  PyMem_FREE(self);
}
 */
static int Behavior_resolve_one(Behavior *self)
{
  if (atomic_decrement(&self->count) != 0LL)
  {
    return 0;
  }

  PRINTDBG("enqueueing behavior\n");

  return PCQueue_enqueue(work_queue, self);
}

static int Behavior_schedule(Behavior *self)
{
  int rc;
  Py_ssize_t i;
  Request *r;
  for (i = 0, r = self->requests; i < self->length; ++i, ++r)
  {
    PRINTDBG("start enqueue request %li\n", i);
    rc = Request_start_enqueue(r, self);
    if (rc != 0)
    {
      return -1;
    }
  }

  for (i = 0, r = self->requests; i < self->length; ++i, ++r)
  {
    PRINTDBG("finish enqueue request %li\n", i);
    Request_finish_enqueue(r);
  }

  rc = Behavior_resolve_one(self);
  if (rc != 0)
  {
    return rc;
  }

  Terminator_increment(terminator);
  return 0;
}

// this must be called while holding the GIL
static void Request_init(Request *self, RegionObject *region)
{
  self->next = NULL;
  self->scheduled = false;
  Py_INCREF(region);
  self->target = region;
}

// this must be called while holding the GIL
/* static void Request_free(Request *self)
{
  PRINTDBG("freeing request with region %s\n", PyUnicode_AsUTF8(self->target->name));
  Py_DECREF(self->target);
} */

static int Request_release(Request *self)
{
  voidptr_t self_ptr = (voidptr_t)self;
  if (self->next == NULL)
  {
    if (atomic_compare_exchange_ptr(&self->target->last, &self_ptr, (voidptr_t)NULL))
    {
      PRINTDBG("No next request\n");
      return 0;
    }

    PRINTDBG("Waiting for next request to be set\n");
    while (self->next == NULL)
    {
      thrd_yield();
    }
  }

  PRINTDBG("Resolving next request\n");
  return Behavior_resolve_one((Behavior *)self->next);
}

static int Request_start_enqueue(Request *self, Behavior *behavior)
{
  Request *prev;
  voidptr_t prev_ptr = atomic_exchange_ptr(&self->target->last, (voidptr_t)self);
  if (prev_ptr == (voidptr_t)NULL)
  {
    PRINTDBG("No previous request\n");
    return Behavior_resolve_one(behavior);
  }

  prev = (Request *)prev_ptr;
  prev->next = behavior;

  while (!prev->scheduled)
  {
    thrd_yield();
  }

  return 0;
}

static void Request_finish_enqueue(Request *self)
{
  self->scheduled = true;
}

static thrd_return_t worker(void *arg)
{
  int rc;
  Py_ssize_t index;
  PyThreadState *ts;
  PyObject *err_type, *err_value, *err_traceback, *veronapy;

  rc = 0;
  err_type = err_value = err_traceback = NULL;

  PRINTDBG("worker starting\n");

  index = (Py_ssize_t)arg;
  ts = subinterpreters[index];
  alloc_id = index + 1;
#ifdef VPY_MULTIGIL
  PyEval_AcquireThread(ts);
#else
  PyEval_AcquireThread(ts);
#endif

  veronapy = PyImport_ImportModule("veronapy");
  if (veronapy == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to import veronapy module");
    rc = -1;
    goto end;
  }

  vpy_state = (VPYState *)PyModule_GetState(veronapy);

  while (!atomic_load_bool(&terminator->set))
  {
    Py_ssize_t i;
    Request *r;
    PyObject *regions;
    Behavior *b;

    ts = PyEval_SaveThread();
    PRINTDBG("waiting for work...\n");
    rc = PCQueue_dequeue(work_queue, &b);
    PyEval_RestoreThread(ts);

    if (rc != 0)
    {
      PRINTDBG("error dequeuing work\n");
      break;
    }

    if (b == NULL)
    {
      PRINTDBG("received NULL behavior\n");
      break;
    }

    PRINTDBG("received work %p\n", b);
    PRINTDBG("preparing regions...\n");
    regions = PyTuple_New(b->length);
    if (regions == NULL)
    {
      PyErr_SetString(PyExc_RuntimeError, "Unable to allocate regions tuple");
      rc = -1;
      break;
    }

    for (i = 0, r = b->requests; i < b->length; ++i, ++r)
    {
      VPY_REGION(r->target);
      PRINTDBG("opening region %s\n", PyUnicode_AsUTF8(region->name));
      region->is_open = true;
      Py_INCREF(region);
      PyTuple_SET_ITEM(regions, i, (PyObject *)region);
    }

    if (err_type == NULL)
    {
      PRINTDBG("Running thunk\n");
      PyObject *result = PyRun_String(PyUnicode_AsUTF8(b->thunk_source), Py_file_input, b->thunk_locals, NULL);
      if (result == NULL)
      {
        PyErr_Fetch(&err_type, &err_value, &err_traceback);
      }
      else
      {
        Py_DECREF(result);
      }

      if (err_type != NULL)
      {
        BehaviorException_new(err_type, err_value, err_traceback);
      }
    }
    else
    {
      PRINTDBG("Exception thrown in worker, skipping thunk\n");
    }
    Py_DECREF(regions);

    PRINTDBG("releasing requests\n");
    for (i = 0, r = b->requests; i < b->length; ++i, ++r)
    {
      VPY_REGION(r->target);
      PRINTDBG("closing region %s\n", PyUnicode_AsUTF8(region->name));
      region->is_open = false;
      ts = PyEval_SaveThread();
      rc = Request_release(r);
      PyEval_RestoreThread(ts);
      if (rc != 0)
      {
        PyErr_SetString(PyExc_RuntimeError, "Unable to release request");
        continue;
      }
    }

    if (rc != 0)
    {
      break;
    }

    PRINTDBG("Decrementing terminator...\n");
    rc = Terminator_decrement(terminator);

    if (rc != 0)
    {
      PyErr_SetString(PyExc_RuntimeError, "Unable to decrement terminator");
      break;
    }
  }

  if (rc != 0)
  {
    PyErr_Fetch(&err_type, &err_value, &err_traceback);
    BehaviorException_new(err_type, err_value, err_traceback);
  }

  PRINTDBG("worker exiting\n");

end:

#ifdef VPY_MULTIGIL
  PyThreadState *nts = PyThreadState_New(ts->interp);
  PyThreadState_Clear(ts);
  PyThreadState_DeleteCurrent();
  subinterpreters[index] = nts;
#else
  PyEval_ReleaseThread(ts);
#endif

  return (thrd_return_t)0;
}

static int set_worker_count()
{
  PyObject *os_module, *os_dict, *function, *result, *key;
  char *worker_count_env;

  worker_count_env = getenv("VPY_WORKER_COUNT");
  if (worker_count_env != NULL)
  {
    PRINTDBG("VPY_WORKER_COUNT: %s\n", worker_count_env);
    worker_count = atoi(worker_count_env);
    if (worker_count < 1)
    {
      PyErr_SetString(PyExc_RuntimeError, "VPY_WORKER_COUNT must be greater than 0");
      return -1;
    }

    if (worker_count > 0)
    {
      return 0;
    }
  }

  PRINTDBG("Unable to load VPY_WORKER_COUNT, using default behavior\n");
  os_module = PyImport_ImportModule("os");
  if (os_module == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to import os module");
    return -1;
  }

  os_dict = PyModule_GetDict(os_module);
  if (os_dict == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to get os module dict");
    return -1;
  }

  key = PyUnicode_FromString("sched_getaffinity");
  if (PyDict_Contains(os_dict, key))
  {
    function = PyDict_GetItemString(os_dict, "sched_getaffinity");
    if (function == NULL)
    {
      Py_DECREF(key);
      PyErr_Format(PyExc_RuntimeError, "Unable to load sched_getaffinity from os module");
      return -1;
    }

    result = PyObject_CallOneArg(function, PyLong_FromLong(0));
    worker_count = PySet_Size(result);
    PRINTDBG("sched_getaffinity returned %li\n", worker_count);
  }
  else
  {
    function = PyDict_GetItemString(os_dict, "cpu_count");
    if (function == NULL)
    {
      Py_DECREF(key);
      PyErr_Format(PyExc_RuntimeError, "Unable to load cpu_count from os module");
      return -1;
    }

    result = PyObject_CallNoArgs(function);
    worker_count = PyLong_AsLong(result);
    PRINTDBG("cpu_count returned %li\n", worker_count);
  }

  Py_DECREF(key);
  Py_DECREF(result);
  if (worker_count < 1)
  {
    PyErr_SetString(PyExc_RuntimeError, "invalid worker count (< 1)");
    return -1;
  }

  return 0;
}

#ifdef VPY_MULTIGIL
static int create_subinterpreters()
{
  Py_ssize_t i;
  PyThreadState *ts;

  PRINTDBG("creating multi-gil subinterpreters\n");

  alloc_id = 0;
  subinterpreters = (PyThreadState **)malloc(sizeof(PyThreadState *) * worker_count);
  for (i = 0; i < worker_count; ++i)
  {
    PyStatus status;
    PyInterpreterConfig config = {
        .use_main_obmalloc = 0,
        .allow_fork = 0,
        .allow_exec = 0,
        .allow_threads = 1,
        .allow_daemon_threads = 0,
        .check_multi_interp_extensions = 1,
        .gil = PyInterpreterConfig_OWN_GIL,
    };
    subinterpreters[i] = NULL;

    ts = PyThreadState_Get();
    status = Py_NewInterpreterFromConfig(subinterpreters + i, &config);
    subinterpreters[i] = PyEval_SaveThread();
    PyEval_RestoreThread(ts);
    if (PyStatus_Exception(status))
    {
      PyObject *exc;
      _PyErr_SetFromPyStatus(status);
      exc = PyErr_GetRaisedException();
      PyErr_SetString(PyExc_RuntimeError, "interpreter creation failed");
      _PyErr_ChainExceptions1(exc);
      return -1;
    }

    PRINTDBG("starting subinterpreter %lu\n", PyInterpreterState_GetID(subinterpreters[i]->interp));
  }

  return 0;
}
#else
static int create_subinterpreters()
{
  Py_ssize_t i;
  PyThreadState *ts;
  PRINTDBG("creating subinterpreters\n");

  subinterpreters = (PyThreadState **)malloc(sizeof(PyThreadState *) * worker_count);
  ts = PyThreadState_Get();
  for (i = 0; i < worker_count; ++i)
  {
    subinterpreters[i] = Py_NewInterpreter();
    if (subinterpreters[i] == NULL)
    {
      PyErr_SetString(PyExc_RuntimeError, "Unable to create subinterpreter");
      return -1;
    }

    PRINTDBG("starting subinterpreter %lu\n", PyInterpreterState_GetID(subinterpreters[i]->interp));
    PyThreadState_Swap(ts);
  }

  return 0;
}
#endif

static void free_subinterpreters()
{
  PyThreadState *ts;
  PRINTDBG("freeing subinterpreters\n");
  for (Py_ssize_t i = 0; i < worker_count; ++i)
  {
    PRINTDBG("ending subinterpreter %lu %p\n", PyInterpreterState_GetID(subinterpreters[i]->interp), subinterpreters[i]);
    ts = PyThreadState_Swap(subinterpreters[i]);
    Py_EndInterpreter(subinterpreters[i]);
    PyThreadState_Swap(ts);
  }

  free(subinterpreters);
}

static int startup_workers()
{
  thrd_t *thr;
  Py_ssize_t i;
  int rc;

  terminator = Terminator_new();
  work_queue = PCQueue_new();
  if (work_queue == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to allocate work queue");
    return -1;
  }

  PRINTDBG("starting workers\n");
  workers = (thrd_t *)malloc(sizeof(thrd_t) * worker_count);
  for (i = 0, thr = workers; i < worker_count; ++i, ++thr)
  {
    rc = thrd_create(thr, worker, (void *)i);
    if (rc != thrd_success)
    {
      PyErr_SetString(PyExc_RuntimeError, "Unable to create worker thread");
      return -1;
    }
  }

  return 0;
}

static int shutdown_workers()
{
  int rc;

  PRINTDBG("shutting down workers\n");

  PRINTDBG("stopping work queue\n");
  Py_BEGIN_ALLOW_THREADS
      rc = PCQueue_stop(work_queue);
  Py_END_ALLOW_THREADS

      if (rc != 0)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to stop work queue");
    return -1;
  }

  PRINTDBG("waiting for workers\n");
  for (Py_ssize_t i = 0; i < worker_count; ++i)
  {
    PRINTDBG("joining worker thread %li\n", i);
    Py_BEGIN_ALLOW_THREADS
        rc = thrd_join(workers[i], NULL);
    Py_END_ALLOW_THREADS

        if (rc != thrd_success)
    {
      PyErr_SetString(PyExc_RuntimeError, "Unable to join worker thread");
      return -1;
    }
  }
  free(workers);

  PRINTDBG("freeing work queue\n");
  PCQueue_free(work_queue);

  PRINTDBG("threading system shutdown complete\n");

  return 0;
}

static WhenObject *When_new(PyTypeObject *type, PyObject *args,
                            PyObject *kwds)
{
  WhenObject *self = (WhenObject *)type->tp_alloc(type, 0);
  if (self == NULL)
  {
    return NULL;
  }

  self->regions = NULL;

  return self;
}

static int When_init(WhenObject *self, PyObject *args, PyObject *kwds)
{
  Py_ssize_t i, num_regions;
  PyObject *r;
  num_regions = PyTuple_Size(args);
  for (i = 0; i < num_regions; ++i)
  {
    r = PyTuple_GetItem(args, i);
    if (r == NULL)
    {
      PyErr_SetString(PyExc_RuntimeError, "Unable to get region from tuple");
      return -1;
    }

    if (!Region_Check(r))
    {
      PyErr_SetString(PyExc_TypeError, "Expected region");
      return -1;
    }

    if (!((RegionObject *)r)->is_shared)
    {
      PyErr_SetString(RegionIsolationError, "Region must be shared");
      return -1;
    }
  }

  Py_INCREF(args);
  self->regions = args;

  PRINTDBG("when initialized @ %p\n", self);
  return 0;
}

static void When_dealloc(WhenObject *self)
{
  PRINTDBG("When_dealloc\n");
  Py_XDECREF(self->regions);
  Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *When_call(WhenObject *self, PyObject *args, PyObject *kwds)
{
  PyObject *thunk, *thunk_source, *thunk_name, *thunk_locals, *thunk_command;
  Behavior *b;
  PyObject *regions;
  Py_ssize_t index;
  int rc;

  PRINTDBG("When_call\n");
  if (PyTuple_Size(args) != 1)
  {
    PyErr_SetString(PyExc_RuntimeError, "Expected one argument");
    return NULL;
  }

  thunk = PyTuple_GetItem(args, 0);
  if (thunk == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to get thunk from tuple");
    return NULL;
  }

  if (!PyCallable_Check(thunk))
  {
    PyErr_SetString(PyExc_TypeError, "Expected callable");
    return NULL;
  }

  regions = PySequence_List(self->regions);
  if (regions == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to convert regions to list");
    return NULL;
  }

  thunk_source = get_source(thunk);
  if (thunk_source == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to get source of thunk");
    return NULL;
  }

  index = PyUnicode_Find(thunk_source, PyUnicode_FromString("def"), 0, PyUnicode_GET_LENGTH(thunk_source), 1);
  if (index == -1)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to find def in thunk source");
    return NULL;
  }

  thunk_source = PyUnicode_Substring(thunk_source, index, PyUnicode_GET_LENGTH(thunk_source));

  thunk_name = PyObject_GetAttrString(thunk, "__name__");
  if (thunk_name == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to get name of thunk");
    return NULL;
  }

  thunk_locals = PyDict_New();
  if (thunk_locals == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to allocate thunk locals");
    return NULL;
  }

  thunk_command = PyUnicode_Concat(thunk_name, PyUnicode_FromString("("));
  // Iterate through the regions, adding them to the code and to the locals
  for (Py_ssize_t i = 0; i < PyList_Size(regions); ++i)
  {
    PyObject *region = PyList_GetItem(regions, i);
    if (region == NULL)
    {
      PyErr_SetString(PyExc_RuntimeError, "Unable to get region from list");
      return NULL;
    }

    PyObject *name = PyUnicode_FromFormat("r_%li", i);
    if (name == NULL)
    {
      PyErr_SetString(PyExc_RuntimeError, "Unable to allocate region name");
      return NULL;
    }

    if (PyDict_SetItem(thunk_locals, name, region) != 0)
    {
      PyErr_SetString(PyExc_RuntimeError, "Unable to set region in thunk locals");
      return NULL;
    }

    PyUnicode_Append(&thunk_command, name);
    if (i < PyList_Size(regions) - 1)
    {
      PyUnicode_Append(&thunk_command, PyUnicode_FromString(", "));
    }
  }

  PyUnicode_Append(&thunk_command, PyUnicode_FromString(")"));

  thunk_source = PyUnicode_Concat(thunk_source, PyUnicode_FromString("\n"));
  thunk_source = PyUnicode_Concat(thunk_source, thunk_command);

  // PRINTDBG("thunk final: %s\n", PyUnicode_AsUTF8(thunk_source));

  PRINTDBG("creating behavior\n");
  b = Behavior_new(thunk_source, thunk_locals, regions);
  Py_DECREF(regions);

  if (b == NULL)
  {
    return NULL;
  }

  PRINTDBG("scheduling behavior\n");
  Py_BEGIN_ALLOW_THREADS
      rc = Behavior_schedule(b);
  Py_END_ALLOW_THREADS

      if (rc != 0)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to schedule behavior");
    return NULL;
  }

  Py_RETURN_TRUE;
}

static PyTypeObject WhenType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "veronapy.when_factory",
    .tp_doc = PyDoc_STR("When factory, returned by when()"),
    .tp_basicsize = sizeof(WhenObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
    .tp_new = (newfunc)When_new,
    .tp_init = (initproc)When_init,
    .tp_dealloc = (destructor)When_dealloc,
    .tp_call = (ternaryfunc)When_call,
};

static PyObject *when(PyObject *module, PyObject *args)
{
  PyObject *when_factory;
  when_factory = PyObject_CallObject((PyObject *)&WhenType, args);
  return when_factory;
}

/***************************************************************/
/*              Isolated object methods                        */
/***************************************************************/

static void Isolated_finalize(PyObject *self)
{
  PyTypeObject *isolated_type = Py_TYPE(self);
  PyTypeObject *type = get_type(isolated_type);
  if (type == NULL)
  {
    PyErr_SetString(PyExc_TypeError,
                    "error obtaining internal type of isolated object");
    return;
  }

  self->ob_type = type;
  Py_DECREF(isolated_type);
}

static PyObject *Isolated_repr(PyObject *self)
{
  PyObject *repr;
  PyTypeObject *isolated_type = Py_TYPE(self);
  VPY_GETTYPE(NULL);
  VPY_GETREGION(NULL);
  VPY_CHECKREGIONOPEN(NULL);

  self->ob_type = type;
  repr = type->tp_repr(self);
  self->ob_type = isolated_type;
  return repr;
}

static PyObject *Isolated_str(PyObject *self)
{
  PyObject *str;
  PyTypeObject *isolated_type = Py_TYPE(self);
  VPY_GETTYPE(NULL);
  VPY_GETREGION(NULL);
  VPY_CHECKREGIONOPEN(NULL);

  self->ob_type = type;
  str = type->tp_str(self);
  self->ob_type = isolated_type;
  return str;
}

static PyObject *Isolated_richcompare(PyObject *self, PyObject *other, int op)
{
  PyObject *result;
  PyTypeObject *isolated_type = Py_TYPE(self);
  VPY_GETTYPE(NULL);
  VPY_GETREGION(NULL);
  VPY_CHECKREGIONOPEN(NULL);

  PRINTDBG("Isolated_richcompare %s\n", PyUnicode_AsUTF8(region->name));

  self->ob_type = type;
  result = type->tp_richcompare(self, other, op);
  self->ob_type = isolated_type;
  return result;
}

static PyObject *Isolated_iter(PyObject *self)
{
  PyObject *iter;
  PyTypeObject *isolated_type = Py_TYPE(self);
  VPY_GETTYPE(NULL);
  VPY_GETREGION(NULL);
  VPY_CHECKREGIONOPEN(NULL);

  self->ob_type = type;
  iter = type->tp_iter(self);
  self->ob_type = isolated_type;
  return iter;
}

static PyObject *Isolated_iternext(PyObject *self)
{
  PyObject *next;
  PyTypeObject *isolated_type = Py_TYPE(self);
  VPY_GETTYPE(NULL);
  VPY_GETREGION(NULL);
  VPY_CHECKREGIONOPEN(NULL);

  self->ob_type = type;
  next = type->tp_iternext(self);
  self->ob_type = isolated_type;
  return next;
}

static Py_hash_t Isolated_hash(PyObject *self)
{
  Py_hash_t hash;
  PyTypeObject *isolated_type = Py_TYPE(self);
  VPY_GETTYPE(0);
  VPY_GETREGION(0);
  VPY_CHECKREGIONOPEN(0);

  self->ob_type = type;
  hash = type->tp_hash(self);
  self->ob_type = isolated_type;
  return hash;
}

/* static int Isolated_traverse(PyObject *self, visitproc visit, void *arg)
{
  int rc;
  PyTypeObject *isolated_type = Py_TYPE(self);
  VPY_GETTYPE(-1);

  self->ob_type = type;
  rc = type->tp_traverse(self, visit, arg);
  self->ob_type = isolated_type;
  return rc;
}

static int Isolated_clear(PyObject *self)
{
  int rc;

  PyTypeObject *isolated_type = Py_TYPE(self);
  VPY_GETTYPE(-1);

  self->ob_type = type;
  rc = type->tp_clear(self);
  self->ob_type = isolated_type;

  return rc;
} */

static PyObject *Isolated_getattro(PyObject *self, PyObject *attr_name)
{
  PyObject *result;
  PyTypeObject *isolated_type = Py_TYPE(self);
  VPY_GETTYPE(NULL);

  if (PyObject_HasAttr((PyObject *)isolated_type, attr_name))
  {
    return PyObject_GenericGetAttr(self, attr_name);
  }

  self->ob_type = type;
  VPY_GETREGION(NULL);

  if (strcmp(PyUnicode_AsUTF8(attr_name), "__region__") == 0)
  {
    result = (PyObject *)region_tag;
  }
  else if (region->is_open)
  {
    result = PyObject_GenericGetAttr(self, attr_name);
  }
  else
  {
    PyErr_SetString(RegionIsolationError, "Region is not open");
    result = NULL;
  }

  self->ob_type = isolated_type;
  return result;
}

static int Isolated_setattro(PyObject *self, PyObject *attr_name,
                             PyObject *arg0)
{
  int rc;

  PyTypeObject *isolated_type = Py_TYPE(self);
  VPY_GETTYPE(-1);
  VPY_GETREGION(-1);

  if (strcmp(PyUnicode_AsUTF8(attr_name), "__region__") == 0)
  {
    PyErr_SetString(PyExc_RuntimeError, "Cannot override region");
    return -1;
  }

  if (!region->is_open)
  {
    PyErr_SetString(RegionIsolationError, "Region is not open");
    return -1;
  }

  VPY_CHECKARG0REGION(-1);

  self->ob_type = type;
  rc = PyObject_GenericSetAttr(self, attr_name, arg0);
  self->ob_type = isolated_type;
  return rc;
}

VPY_LENFUNC(tp_as_mapping, mp_length)
VPY_BINARYFUNC(tp_as_mapping, mp_subscript)
VPY_OBJOBJARGPROC(tp_as_mapping, mp_ass_subscript);
VPY_LENFUNC(tp_as_sequence, sq_length);
VPY_BINARYFUNC(tp_as_sequence, sq_concat);
VPY_SSIZEARGFUNC(tp_as_sequence, sq_repeat);
VPY_SSIZEARGFUNC(tp_as_sequence, sq_item);
VPY_SSIZEOBJARGPROC(tp_as_sequence, sq_ass_item);
VPY_OBJOBJPROC(tp_as_sequence, sq_contains);
VPY_BINARYFUNC(tp_as_sequence, sq_inplace_concat);
VPY_SSIZEARGFUNC(tp_as_sequence, sq_inplace_repeat);
VPY_BINARYFUNC(tp_as_number, nb_add);
VPY_BINARYFUNC(tp_as_number, nb_subtract);
VPY_BINARYFUNC(tp_as_number, nb_multiply);
VPY_BINARYFUNC(tp_as_number, nb_remainder);
VPY_BINARYFUNC(tp_as_number, nb_divmod);
VPY_TERNARYFUNC(tp_as_number, nb_power);
VPY_UNARYFUNC(tp_as_number, nb_negative);
VPY_UNARYFUNC(tp_as_number, nb_positive);
VPY_UNARYFUNC(tp_as_number, nb_absolute);
VPY_INQUIRY(tp_as_number, nb_bool);
VPY_UNARYFUNC(tp_as_number, nb_invert);
VPY_BINARYFUNC(tp_as_number, nb_lshift);
VPY_BINARYFUNC(tp_as_number, nb_rshift);
VPY_BINARYFUNC(tp_as_number, nb_and);
VPY_BINARYFUNC(tp_as_number, nb_xor);
VPY_BINARYFUNC(tp_as_number, nb_or);
VPY_UNARYFUNC(tp_as_number, nb_int);
VPY_UNARYFUNC(tp_as_number, nb_float);
VPY_BINARYFUNC(tp_as_number, nb_inplace_add);
VPY_BINARYFUNC(tp_as_number, nb_inplace_subtract);
VPY_BINARYFUNC(tp_as_number, nb_inplace_multiply);
VPY_BINARYFUNC(tp_as_number, nb_inplace_remainder);
VPY_TERNARYFUNC(tp_as_number, nb_inplace_power);
VPY_BINARYFUNC(tp_as_number, nb_inplace_lshift);
VPY_BINARYFUNC(tp_as_number, nb_inplace_rshift);
VPY_BINARYFUNC(tp_as_number, nb_inplace_and);
VPY_BINARYFUNC(tp_as_number, nb_inplace_xor);
VPY_BINARYFUNC(tp_as_number, nb_inplace_or);
VPY_BINARYFUNC(tp_as_number, nb_floor_divide);
VPY_BINARYFUNC(tp_as_number, nb_true_divide);
VPY_BINARYFUNC(tp_as_number, nb_inplace_floor_divide);
VPY_BINARYFUNC(tp_as_number, nb_inplace_true_divide);
VPY_UNARYFUNC(tp_as_number, nb_index);
VPY_BINARYFUNC(tp_as_number, nb_matrix_multiply);
VPY_BINARYFUNC(tp_as_number, nb_inplace_matrix_multiply);

static Py_ssize_t ssize_length(Py_ssize_t value)
{
  if (value < 10)
  {
    return 1;
  }

  if (value < 100)
  {
    return 2;
  }

  if (value < 1000)
  {
    return 3;
  }

  Py_ssize_t length = 3;
  value /= 1000;
  do
  {
    ++length;
    value /= 10;
  } while (value != 0);

  return length;
}

static int make_immortal(PyObject *obj)
{
  Py_SET_REFCNT(obj, _Py_IMMORTAL_REFCNT);

  PyObject **dict_ptr = _PyObject_GetDictPtr(obj);
  if (dict_ptr == NULL)
  {
    return 0;
  }

  PyObject *dict = *dict_ptr;
  if (dict == NULL)
  {
    return 0;
  }

  PyObject *keys = PyDict_Keys(dict);
  if (keys == NULL)
  {
    return -1;
  }

  Py_ssize_t length = PyList_Size(keys);
  for (Py_ssize_t i = 0; i < length; ++i)
  {
    PyObject *key = PyList_GetItem(keys, i);
    if (key == NULL)
    {
      return -1;
    }

    PyObject *value = PyDict_GetItem(*dict_ptr, key);
    if (value == NULL)
    {
      return -1;
    }

    if (make_immortal(value) < 0)
    {
      return -1;
    }
  }

  return 0;
}

// Creates a new isolated type for the given type
// The new type will wrap all of the methods of the given type
// to enforce that the region containing an object of this
// type is open. The resulting type will wrap a new type object
// that is a copy of the given type object.
static PyTypeObject *isolate_type(PyTypeObject *type)
{
  char *name;
  PyType_Slot slots[] = {
      {Py_tp_repr, NULL},
      {Py_tp_str, NULL},
      {Py_tp_traverse, NULL},
      {Py_tp_clear, NULL},
      {Py_tp_hash, NULL},
      {Py_tp_iter, NULL},
      {Py_tp_iternext, NULL},
      {Py_tp_richcompare, NULL},
      {Py_mp_length, NULL},
      {Py_mp_subscript, NULL},
      {Py_mp_ass_subscript, NULL},
      {Py_sq_length, NULL},
      {Py_sq_concat, NULL},
      {Py_sq_repeat, NULL},
      {Py_sq_item, NULL},
      {Py_sq_ass_item, NULL},
      {Py_sq_contains, NULL},
      {Py_sq_inplace_concat, NULL},
      {Py_sq_inplace_repeat, NULL},
      {Py_nb_add, NULL},
      {Py_nb_subtract, NULL},
      {Py_nb_multiply, NULL},
      {Py_nb_remainder, NULL},
      {Py_nb_divmod, NULL},
      {Py_nb_power, NULL},
      {Py_nb_negative, NULL},
      {Py_nb_positive, NULL},
      {Py_nb_absolute, NULL},
      {Py_nb_bool, NULL},
      {Py_nb_invert, NULL},
      {Py_nb_lshift, NULL},
      {Py_nb_rshift, NULL},
      {Py_nb_and, NULL},
      {Py_nb_xor, NULL},
      {Py_nb_or, NULL},
      {Py_nb_int, NULL},
      {Py_nb_float, NULL},
      {Py_nb_inplace_add, NULL},
      {Py_nb_inplace_subtract, NULL},
      {Py_nb_inplace_multiply, NULL},
      {Py_nb_inplace_remainder, NULL},
      {Py_nb_inplace_power, NULL},
      {Py_nb_inplace_lshift, NULL},
      {Py_nb_inplace_rshift, NULL},
      {Py_nb_inplace_and, NULL},
      {Py_nb_inplace_xor, NULL},
      {Py_nb_inplace_or, NULL},
      {Py_nb_floor_divide, NULL},
      {Py_nb_true_divide, NULL},
      {Py_nb_inplace_floor_divide, NULL},
      {Py_nb_inplace_true_divide, NULL},
      {Py_nb_index, NULL},
      {Py_nb_matrix_multiply, NULL},
      {Py_nb_inplace_matrix_multiply, NULL},
      {Py_tp_finalize, Isolated_finalize},
      {Py_tp_getattro, Isolated_getattro},
      {Py_tp_setattro, Isolated_setattro},
      {0, NULL} /* Sentinel */
  };

  const int num_tp_slots = 8;
  const int num_mp_slots = 3;
  const int num_sq_slots = 8;

  if (type->tp_repr != NULL)
  {
    slots[0].pfunc = Isolated_repr;
  }
  if (type->tp_str != NULL)
  {
    slots[1].pfunc = Isolated_str;
  }
  if (type->tp_traverse != NULL)
  {
    // slots[2].pfunc = Isolated_traverse;
  }
  if (type->tp_clear != NULL)
  {
    // slots[3].pfunc = Isolated_clear;
  }
  if (type->tp_hash != NULL)
  {
    slots[4].pfunc = Isolated_hash;
  }
  if (type->tp_iter != NULL)
  {
    slots[5].pfunc = Isolated_iter;
  }
  if (type->tp_iternext != NULL)
  {
    slots[6].pfunc = Isolated_iternext;
  }
  if (type->tp_richcompare != NULL)
  {
    slots[7].pfunc = Isolated_richcompare;
  }

  if (type->tp_as_mapping != NULL)
  {
    PyMappingMethods *mp_methods = type->tp_as_mapping;
    PyType_Slot *mp_slots = slots + num_tp_slots;
    if (mp_methods->mp_length != NULL)
    {
      mp_slots[0].pfunc = Isolated_mp_length;
    }
    if (mp_methods->mp_subscript != NULL)
    {
      mp_slots[1].pfunc = Isolated_mp_subscript;
    }
    if (mp_methods->mp_ass_subscript != NULL)
    {
      mp_slots[2].pfunc = Isolated_mp_ass_subscript;
    }
  }

  if (type->tp_as_sequence != NULL)
  {
    PySequenceMethods *sq_methods = type->tp_as_sequence;
    PyType_Slot *sq_slots = slots + num_tp_slots + num_mp_slots;
    if (sq_methods->sq_length != NULL)
    {
      sq_slots[0].pfunc = Isolated_sq_length;
    }
    if (sq_methods->sq_concat != NULL)
    {
      sq_slots[1].pfunc = Isolated_sq_concat;
    }
    if (sq_methods->sq_repeat != NULL)
    {
      sq_slots[2].pfunc = Isolated_sq_repeat;
    }
    if (sq_methods->sq_item != NULL)
    {
      sq_slots[3].pfunc = Isolated_sq_item;
    }
    if (sq_methods->sq_ass_item != NULL)
    {
      sq_slots[4].pfunc = Isolated_sq_ass_item;
    }
    if (sq_methods->sq_contains != NULL)
    {
      sq_slots[5].pfunc = Isolated_sq_contains;
    }
    if (sq_methods->sq_inplace_concat != NULL)
    {
      sq_slots[6].pfunc = Isolated_sq_inplace_concat;
    }
    if (sq_methods->sq_inplace_repeat != NULL)
    {
      sq_slots[7].pfunc = Isolated_sq_inplace_repeat;
    }
  }

  if (type->tp_as_number != NULL)
  {
    PyNumberMethods *nb_methods = type->tp_as_number;
    PyType_Slot *nb_slots = slots + num_tp_slots + num_mp_slots + num_sq_slots;
    if (nb_methods->nb_add != NULL)
    {
      nb_slots[0].pfunc = Isolated_nb_add;
    }
    if (nb_methods->nb_subtract != NULL)
    {
      nb_slots[1].pfunc = Isolated_nb_subtract;
    }
    if (nb_methods->nb_multiply != NULL)
    {
      nb_slots[2].pfunc = Isolated_nb_multiply;
    }
    if (nb_methods->nb_remainder != NULL)
    {
      nb_slots[3].pfunc = Isolated_nb_remainder;
    }
    if (nb_methods->nb_divmod != NULL)
    {
      nb_slots[4].pfunc = Isolated_nb_divmod;
    }
    if (nb_methods->nb_power != NULL)
    {
      nb_slots[5].pfunc = Isolated_nb_power;
    }
    if (nb_methods->nb_negative != NULL)
    {
      nb_slots[6].pfunc = Isolated_nb_negative;
    }
    if (nb_methods->nb_positive != NULL)
    {
      nb_slots[7].pfunc = Isolated_nb_positive;
    }
    if (nb_methods->nb_absolute != NULL)
    {
      nb_slots[8].pfunc = Isolated_nb_absolute;
    }
    if (nb_methods->nb_bool != NULL)
    {
      nb_slots[9].pfunc = Isolated_nb_bool;
    }
    if (nb_methods->nb_invert != NULL)
    {
      nb_slots[10].pfunc = Isolated_nb_invert;
    }
    if (nb_methods->nb_lshift != NULL)
    {
      nb_slots[11].pfunc = Isolated_nb_lshift;
    }
    if (nb_methods->nb_rshift != NULL)
    {
      nb_slots[12].pfunc = Isolated_nb_rshift;
    }
    if (nb_methods->nb_and != NULL)
    {
      nb_slots[13].pfunc = Isolated_nb_and;
    }
    if (nb_methods->nb_xor != NULL)
    {
      nb_slots[14].pfunc = Isolated_nb_xor;
    }
    if (nb_methods->nb_or != NULL)
    {
      nb_slots[15].pfunc = Isolated_nb_or;
    }
    if (nb_methods->nb_int != NULL)
    {
      nb_slots[16].pfunc = Isolated_nb_int;
    }
    if (nb_methods->nb_float != NULL)
    {
      nb_slots[17].pfunc = Isolated_nb_float;
    }
    if (nb_methods->nb_inplace_add != NULL)
    {
      nb_slots[18].pfunc = Isolated_nb_inplace_add;
    }
    if (nb_methods->nb_inplace_subtract != NULL)
    {
      nb_slots[19].pfunc = Isolated_nb_inplace_subtract;
    }
    if (nb_methods->nb_inplace_multiply != NULL)
    {
      nb_slots[20].pfunc = Isolated_nb_inplace_multiply;
    }
    if (nb_methods->nb_inplace_remainder != NULL)
    {
      nb_slots[21].pfunc = Isolated_nb_inplace_remainder;
    }
    if (nb_methods->nb_inplace_power != NULL)
    {
      nb_slots[22].pfunc = Isolated_nb_inplace_power;
    }
    if (nb_methods->nb_inplace_lshift != NULL)
    {
      nb_slots[23].pfunc = Isolated_nb_inplace_lshift;
    }
    if (nb_methods->nb_inplace_rshift != NULL)
    {
      nb_slots[24].pfunc = Isolated_nb_inplace_rshift;
    }
    if (nb_methods->nb_inplace_and != NULL)
    {
      nb_slots[25].pfunc = Isolated_nb_inplace_and;
    }
    if (nb_methods->nb_inplace_xor != NULL)
    {
      nb_slots[26].pfunc = Isolated_nb_inplace_xor;
    }
    if (nb_methods->nb_inplace_or != NULL)
    {
      nb_slots[27].pfunc = Isolated_nb_inplace_or;
    }
    if (nb_methods->nb_floor_divide != NULL)
    {
      nb_slots[28].pfunc = Isolated_nb_floor_divide;
    }
    if (nb_methods->nb_true_divide != NULL)
    {
      nb_slots[29].pfunc = Isolated_nb_true_divide;
    }
    if (nb_methods->nb_inplace_floor_divide != NULL)
    {
      nb_slots[30].pfunc = Isolated_nb_inplace_floor_divide;
    }
    if (nb_methods->nb_inplace_true_divide != NULL)
    {
      nb_slots[31].pfunc = Isolated_nb_inplace_true_divide;
    }
    if (nb_methods->nb_index != NULL)
    {
      nb_slots[32].pfunc = Isolated_nb_index;
    }
    if (nb_methods->nb_matrix_multiply != NULL)
    {
      nb_slots[33].pfunc = Isolated_nb_matrix_multiply;
    }
    if (nb_methods->nb_inplace_matrix_multiply != NULL)
    {
      nb_slots[34].pfunc = Isolated_nb_inplace_matrix_multiply;
    }
  }

  const char *name_format = "interp_%li.isolated.%s";
  name = (char *)malloc(strlen(name_format) + strlen(type->tp_name) + ssize_length(alloc_id) + 1);
  sprintf(name, name_format, alloc_id, type->tp_name);
  PyType_Spec spec = {
      .name = name,
      .basicsize = (int)type->tp_basicsize,
      .itemsize = (int)type->tp_itemsize,
      .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HEAPTYPE,
      .slots = slots,
  };

  Py_INCREF((PyObject *)type);
  PyTypeObject *isolated_type = (PyTypeObject *)PyType_FromSpecWithBases(&spec, (PyObject *)type);
  if (isolated_type == NULL)
  {
    PyErr_SetString(PyExc_RuntimeError, "Unable to create isolated type");
    return NULL;
  }

  PRINTDBG("created isolated type %p for %s\n", isolated_type, type->tp_name);

  PyObject *source = get_source((PyObject *)type);
  if (source == NULL)
  {
    // this is a builtin type or an extension type. We freeze it and hope for the best.
    PyErr_Clear();

    Py_INCREF((PyObject *)type);
    if (PyDict_SetItemString(PyType_GetDict(isolated_type), "__isolated__", (PyObject *)type) < 0)
    {
      Py_DECREF((PyObject *)isolated_type);
      PyErr_SetString(PyExc_TypeError,
                      "error adding internal type to isolated type dictionary");
      return NULL;
    }

    type->tp_flags = Py_TPFLAGS_IMMUTABLETYPE | type->tp_flags;
    if (make_immortal((PyObject *)type) < 0)
    {
      PyErr_SetString(PyExc_RuntimeError, "Unable to make type immortal");
      return NULL;
    }

    return isolated_type;
  }

  long long type_id = atomic_increment(&frozen_type_count);
  PyObject *type_id_long = PyLong_FromLongLong(type_id);

  if (PyDict_SetItemString(PyType_GetDict(isolated_type), "__isolated__", type_id_long) < 0)
  {
    Py_DECREF((PyObject *)isolated_type);
    PyErr_SetString(PyExc_TypeError,
                    "error adding frozen type source to isolated type dictionary");
    return NULL;
  }

  PyObject *type_tuple = PyTuple_Pack(2, (PyObject *)type, source);
  if (type_tuple == NULL)
  {
    Py_DECREF((PyObject *)isolated_type);
    PyErr_SetString(PyExc_TypeError,
                    "error creating frozen type tuple");
    return NULL;
  }

  if (PyDict_SetItem(vpy_state->frozen_types, type_id_long, type_tuple) < 0)
  {
    Py_DECREF((PyObject *)isolated_type);
    PyErr_SetString(PyExc_TypeError,
                    "error adding frozen type to frozen types dictionary");
    return NULL;
  }

  if (!ht_set(global_frozen_types, (voidptr_t)type_id, (voidptr_t)source))
  {
    Py_DECREF((PyObject *)isolated_type);
    PyErr_SetString(PyExc_TypeError,
                    "error adding frozen type source to global frozen types dictionary");
    return NULL;
  }

  return isolated_type;
}

/***************************************************************/
/*              Merge struct and methods                       */
/***************************************************************/

typedef struct merge_object_s
{
  PyObject_HEAD;
  PyObject *objects;
} MergeObject;

static PyObject *Merge_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
  MergeObject *self;
  self = (MergeObject *)type->tp_alloc(type, 0);
  if (self != NULL)
  {
    self->objects = NULL;
  }

  return (PyObject *)self;
}

static void Merge_dealloc(MergeObject *self)
{
  Py_XDECREF(self->objects);
  Py_TYPE(self)->tp_free((PyObject *)self);
}

static int Merge_init(MergeObject *self, PyObject *args, PyObject *kwds)
{
  PyObject *objects = NULL;

  if (!PyArg_ParseTuple(args, "O", &objects))
    return -1;

  if (objects)
  {
    Py_INCREF(objects);
    self->objects = objects;
  }

  return 0;
}

static PyObject *Merge_getattro(MergeObject *self, PyObject *name)
{
  return PyObject_GetItem(self->objects, name);
}

static PyTypeObject MergeType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "veronapy.merge",
    .tp_doc = PyDoc_STR("Merge object"),
    .tp_basicsize = sizeof(MergeObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = Merge_new,
    .tp_init = (initproc)Merge_init,
    .tp_dealloc = (destructor)Merge_dealloc,
    .tp_getattro = (getattrofunc)Merge_getattro,
};

/***************************************************************/
/*              Region object methods                          */
/***************************************************************/

/* static int Region_clear(RegionObject *self)
{
  Py_CLEAR(self->objects);
  Py_CLEAR(self->types);
  Py_CLEAR(self->alias);
  Py_CLEAR(self->parent);
  return 0;
}

static int Region_traverse(RegionObject *self, visitproc visit, void *arg)
{
  Py_VISIT(self->objects);
  Py_VISIT(self->types);
  Py_VISIT(self->alias);
  Py_VISIT(self->parent);
  return 0;
} */

static void Region_dealloc(RegionObject *self)
{
  // PyObject_GC_UnTrack(self);
  // Region_clear(self);
  PRINTDBG("deallocating region %llu\n", self->id);
  Py_XDECREF(self->name);
  Py_XDECREF(self->alias);
  Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *Region_new(PyTypeObject *type, PyObject *args,
                            PyObject *kwds)
{
  RegionObject *self;
  self = (RegionObject *)type->tp_alloc(type, 0);
  if (self != NULL)
  {
    self->name = PyUnicode_FromString("");
    if (self->name == NULL)
    {
      Py_DECREF(self);
      return NULL;
    }

    // The region is its own alias by default
    Py_INCREF((PyObject *)self);
    self->alias = (PyObject *)self;
    self->parent = NULL;
    self->is_open = false;
    self->is_shared = false;
    self->id = 0;
    self->objects = PyDict_New();
    if (self->objects == NULL)
    {
      Py_DECREF(self);
      return NULL;
    }
  }

  return (PyObject *)self;
}

static int Region_init(RegionObject *self, PyObject *args, PyObject *kwds)
{
  static char *kwlist[] = {"name", NULL};
  PyObject *name = NULL, *tmp, *typedict, *key;

  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|O", kwlist, &name))
    return -1;

  self->id = atomic_increment(&region_identity);
  key = PyUnicode_FromFormat("region_%llu", self->id);

  if (name)
  {
    tmp = self->name;
    Py_INCREF(name);
    self->name = name;
    Py_XDECREF(tmp);
  }
  else
  {
    self->name = PyUnicode_FromFormat("region_%llu", self->id);
    if (self->name == NULL)
    {
      Py_DECREF(self);
      return -1;
    }
  }

  typedict = PyType_GetDict(self->ob_base.ob_type);
  if (typedict == NULL)
  {
    Py_DECREF(self);
    return -1;
  }

  Py_INCREF(self);
  if (PyDict_SetItem(typedict, key, (PyObject *)self) < 0)
  {
    Py_DECREF(self);
    return -1;
  }

  PRINTDBG("region init %s %lu @ %p\n", PyUnicode_AsUTF8(self->name), self->id, self);

  return 0;
}

static PyObject *Region_getname(RegionObject *self, void *closure)
{
  VPY_REGION(self);
  Py_INCREF(region->name);
  return region->name;
}

static int Region_setname(RegionObject *self, PyObject *value, void *closure)
{
  VPY_REGION(self);
  PyObject *tmp;
  if (value == NULL)
  {
    PyErr_SetString(PyExc_TypeError, "Cannot delete the name attribute");
    return -1;
  }

  if (!PyUnicode_Check(value))
  {
    PyErr_SetString(PyExc_TypeError,
                    "The name attribute value must be a string");
    return -1;
  }

  tmp = region->name;
  Py_INCREF(value);
  region->name = value;
  Py_XDECREF(tmp);
  return 0;
}

static PyObject *Region_getidentity(RegionObject *self, void *closure)
{
  VPY_REGION(self);
  return PyLong_FromUnsignedLongLong(region->id);
}

static PyObject *Region_getparent(RegionObject *self, void *closure)
{
  VPY_REGION(self);
  Py_INCREF(region->parent);
  return region->parent;
}

static PyObject *Region_getisopen(RegionObject *self, void *closure)
{
  VPY_REGION(self);
  return PyBool_FromLong(region->is_open);
}

static PyObject *Region_getisshared(RegionObject *self, void *closure)
{
  VPY_REGION(self);
  return PyBool_FromLong(region->is_shared);
}

static PyObject *Region_getisfree(RegionObject *self, void *closure)
{
  VPY_REGION(self);
  return PyBool_FromLong(is_free(region));
}

static PyGetSetDef Region_getsetters[] = {
    {"name", (getter)Region_getname, (setter)Region_setname,
     "Human-readable name for the region", NULL},
    {"id", (getter)Region_getidentity, NULL,
     "The immutable, unique identity for the region", NULL},
    {"parent", (getter)Region_getparent, NULL, "The parent region", NULL},
    {"is_open", (getter)Region_getisopen, NULL,
     "Whether the region is currently open", NULL},
    {"is_shared", (getter)Region_getisshared, NULL,
     "Whether the region is shared", NULL},
    {"is_free", (getter)Region_getisfree, NULL, "Whether the region is free",
     NULL},
    {NULL} /* Sentinel */
};

static PyObject *Region_merge(RegionObject *self, PyObject *args,
                              PyObject *kwds)
{
  PyObject *arg;
  RegionObject *other;
  VPY_REGION(self);

  if (!PyArg_ParseTuple(args, "O", &arg))
    return NULL;

  if (!Region_Check(arg))
  {
    PyErr_SetString(PyExc_TypeError, "Argument must be a region");
    return NULL;
  }

  other = (RegionObject *)arg;

  if (!region->is_open)
  {
    PyErr_SetString(RegionIsolationError, "Region is not open");
    return NULL;
  }

  if (!is_free(region))
  {
    PyErr_SetString(RegionIsolationError, "Region is not free");
    return NULL;
  }

  other->alias = (PyObject *)region;
  Py_INCREF(region);

  PyObject *argList = Py_BuildValue("(O)", other->objects);
  PyObject *merged = PyObject_Call((PyObject *)&MergeType, argList, NULL);
  if (merged == NULL)
  {
    return NULL;
  }

  Py_INCREF(merged);
  return merged;
}

static PyObject *Region_makeshareable(RegionObject *self, PyObject *Py_UNUSED(ignored))
{
  VPY_REGION(self);

  region->is_shared = true;
  region->last = 0;
  Py_INCREF(self);
  return (PyObject *)self;
}

static PyObject *Region_detachall(RegionObject *self, PyObject *args)
{
  PyObject *objects;
  RegionObject *detached;
  VPY_REGION(self);

  if (!region->is_open)
  {
    PyErr_SetString(RegionIsolationError, "Region must be open");
    return NULL;
  }

  if (!region->is_shared)
  {
    PyErr_SetString(RegionIsolationError, "Region must be shared");
    return NULL;
  }

  detached = (RegionObject *)PyObject_CallObject((PyObject *)self->ob_base.ob_type, args);
  if (detached == NULL)
  {
    return NULL;
  }

  objects = self->objects;
  self->objects = detached->objects;
  detached->objects = objects;

  Py_INCREF(detached);
  return (PyObject *)detached;
}

static PyObject *Region_str(RegionObject *self, PyObject *Py_UNUSED(ignored))
{
  VPY_REGION(self);
  return PyUnicode_FromFormat("Region(%S)", region->name);
}

static PyObject *Region_repr(RegionObject *self, PyObject *Py_UNUSED(ignored))
{
  VPY_REGION(self);
  return PyUnicode_FromFormat("Region(%S%s%s)", region->name,
                              region->id, region->is_open ? " open" : "",
                              region->is_shared ? " shared" : "");
}

static PyObject *Region_enter(RegionObject *self,
                              PyObject *Py_UNUSED(ignored))
{
  VPY_REGION(self);
  region->is_open = true;
  Py_INCREF((PyObject *)region);
  return (PyObject *)region;
}

static PyObject *Region_exit(RegionObject *self, PyObject *args,
                             PyObject *kwds)
{
  VPY_REGION(self);
  PyObject *type, *value, *traceback;
  if (!PyArg_ParseTuple(args, "OOO", &type, &value, &traceback))
    return NULL;

  if (type == Py_None)
  {
    region->is_open = false;
  }
  else
  {
    Py_INCREF(type);
    Py_INCREF(value);
    Py_INCREF(traceback);
    PyErr_Restore(type, value, traceback);
    return NULL;
  }

  Py_RETURN_FALSE;
}

static PyMethodDef Region_methods[] = {
    {"__enter__", (PyCFunction)Region_enter, METH_NOARGS, "Enter the region"},
    {"__exit__", (PyCFunction)Region_exit, METH_VARARGS,
     "Exit the region, returning a Boolean flag indicating whether any "
     "exception that occurred should be suppressed."},
    {"merge", (PyCFunction)Region_merge, METH_VARARGS,
     "Merge the other region into this one"},
    {"make_shareable", (PyCFunction)Region_makeshareable, METH_NOARGS,
     "Make the region shareable"},
    {"detach_all", (PyCFunction)Region_detachall, METH_VARARGS,
     "Detach all objects from the region"},
    {NULL} /* Sentinel */
};

static PyObject *Region_getattro(RegionObject *self, PyObject *attr_name)
{
  VPY_REGION(self);
  for (int i = 0; REGION_ATTRS[i] != NULL; i++)
  {
    if (strcmp(PyUnicode_AsUTF8(attr_name), REGION_ATTRS[i]) == 0)
    {
      return PyObject_GenericGetAttr((PyObject *)region, attr_name);
    }
  }

  if (!region->is_open)
  {
    PyErr_SetString(RegionIsolationError, "Region is not open");
    return NULL;
  }

  PyObject *obj = PyDict_GetItemWithError(region->objects, attr_name);
  if (obj == NULL)
  {
    Py_RETURN_NONE;
  }

  Py_INCREF(obj);
  return obj;
}

static int Region_setattro(RegionObject *self, PyObject *attr_name,
                           PyObject *value)
{
  int rc;
  VPY_REGION(self);
  for (int i = 0; REGION_ATTRS[i] != NULL; i++)
  {
    if (strcmp(PyUnicode_AsUTF8(attr_name), REGION_ATTRS[i]) == 0)
    {
      return PyObject_GenericSetAttr((PyObject *)region, attr_name, value);
    }
  }

  if (!region->is_open)
  {
    PyErr_SetString(RegionIsolationError, "Region is not open");
    return -1;
  }

  RegionObject *value_region = region_of(value);
  if (value_region == NULL)
  {
    rc = capture_object(region, value);
    if (rc < 0)
    {
      return rc;
    }

    value_region = region;
  }

  if (value_region == region)
  {
    Py_INCREF(value);
    rc = PyDict_SetItem(region->objects, attr_name, value);
    return rc;
  }

  PyErr_SetString(RegionIsolationError, "Value belongs to another region");
  return -1;
}

static Py_hash_t Region_hash(RegionObject *self)
{
  VPY_REGION(self);
  return (Py_hash_t)region->id;
}

static PyObject *Region_richcompare(PyObject *lhs, PyObject *rhs, int op)
{
  RegionObject *lhs_region, *rhs_region;
  if (!Region_Check(rhs))
  {
    Py_RETURN_NOTIMPLEMENTED;
  }

  lhs_region = (RegionObject *)lhs;
  rhs_region = (RegionObject *)rhs;
  if (lhs_region->alias != lhs)
  {
    lhs_region = resolve_region(lhs_region);
  }
  if (rhs_region->alias != rhs)
  {
    rhs_region = resolve_region(rhs_region);
  }

  Py_RETURN_RICHCOMPARE(lhs_region->id, rhs_region->id, op);
}

static PyTypeObject RegionType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "veronapy.region",
    .tp_doc = PyDoc_STR("Region object"),
    .tp_basicsize = sizeof(RegionObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT, //  | Py_TPFLAGS_HAVE_GC
    .tp_new = Region_new,
    .tp_init = (initproc)Region_init,
    .tp_dealloc = (destructor)Region_dealloc,
    .tp_methods = Region_methods,
    .tp_getset = Region_getsetters,
    .tp_repr = (reprfunc)Region_repr,
    .tp_str = (reprfunc)Region_str,
    .tp_setattro = (setattrofunc)Region_setattro,
    .tp_getattro = (getattrofunc)Region_getattro,
    //.tp_traverse = (traverseproc)Region_traverse,
    //.tp_clear = (inquiry)Region_clear,
    .tp_hash = (hashfunc)Region_hash,
    .tp_richcompare = (richcmpfunc)Region_richcompare,
};

static bool Region_Check(PyObject *obj)
{
  PyTypeObject *obj_type = Py_TYPE(obj);
  PyTypeObject *region_type = (PyTypeObject *)&RegionType;
  return obj_type == region_type;
}

/***************************************************************/
/*                 RegionTag setup                             */
/***************************************************************/

static void RegionTag_dealloc(RegionTagObject *self)
{
  Py_XDECREF(self->region);
  Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *RegionTag_new(PyTypeObject *type, PyObject *args,
                               PyObject *kwds)
{
  RegionTagObject *self;
  self = (RegionTagObject *)type->tp_alloc(type, 0);
  if (self != NULL)
  {
    self->region = NULL;
  }

  return (PyObject *)self;
}

static int RegionTag_init(RegionTagObject *self, PyObject *args, PyObject *kwds)
{
  PyObject *region = NULL;

  if (!PyArg_ParseTuple(args, "O", &region))
    return -1;

  self->region = (RegionObject *)region;

  return 0;
}

static PyObject *RegionTag_str(RegionTagObject *self, PyObject *Py_UNUSED(ignored))
{
  VPY_REGION(self->region);
  return PyUnicode_FromFormat("RegionTag(%S)", region->name, region->id);
}

static PyObject *RegionTag_repr(RegionTagObject *self, PyObject *Py_UNUSED(ignored))
{
  VPY_REGION(self->region);
  return PyUnicode_FromFormat("RegionTag(%S%s%s)", region->name,
                              region->id, region->is_open ? " open" : "",
                              region->is_shared ? " shared" : "");
}

static PyObject *RegionTag_richcompare(PyObject *lhs, PyObject *rhs, int op)
{
  RegionTagObject *lhs_tag, *rhs_tag;
  RegionObject *lhs_region, *rhs_region;
  if (Region_Check(rhs))
  {
    rhs_region = (RegionObject *)rhs;
  }
  else if (rhs->ob_type == &RegionTagType)
  {
    rhs_tag = (RegionTagObject *)rhs;
    rhs_region = rhs_tag->region;
  }
  else
  {
    Py_RETURN_NOTIMPLEMENTED;
  }

  lhs_tag = (RegionTagObject *)lhs;
  lhs_region = lhs_tag->region;

  if (lhs_region->alias != lhs)
  {
    lhs_region = resolve_region(lhs_region);
  }
  if (rhs_region->alias != rhs)
  {
    rhs_region = resolve_region(rhs_region);
  }

  Py_RETURN_RICHCOMPARE(lhs_region->id, rhs_region->id, op);
}

static PyTypeObject RegionTagType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "veronapy.regiontag",
    .tp_doc = PyDoc_STR("Region tag object"),
    .tp_basicsize = sizeof(RegionTagObject),
    .tp_itemsize = 0,
    .tp_new = RegionTag_new,
    .tp_init = (initproc)RegionTag_init,
    .tp_dealloc = (destructor)RegionTag_dealloc,
    .tp_repr = (reprfunc)RegionTag_repr,
    .tp_str = (reprfunc)RegionTag_str,
    .tp_richcompare = (richcmpfunc)RegionTag_richcompare,
};

/***************************************************************/
/*                  Module setup                               */
/***************************************************************/

static int VPY_run()
{
  int rc;
  bool expected = false;
  if (!atomic_compare_exchange_bool(&running, &expected, true))
  {
    return 0;
  }

  global_imm_object_regions = ht_create(128, true);
  if (global_imm_object_regions == NULL)
  {
    return -1;
  }

  global_frozen_types = ht_create(128, true);
  if (global_frozen_types == NULL)
  {
    return -1;
  }

  rc = set_worker_count();
  if (rc != 0)
  {
    return rc;
  }

  rc = create_subinterpreters();
  if (rc != 0)
  {
    return rc;
  }

  rc = startup_workers();
  if (rc != 0)
  {
    return rc;
  }

  return 0;
}

static int VPY_wait()
{
  int rc;
  bool expected = true;
  if (!atomic_compare_exchange_bool(&running, &expected, false))
  {
    return 0;
  }

  BehaviorException *ex;
  PRINTDBG("wait\n");
  PRINTDBG("Waiting for terminator to be set\n");
  rc = Terminator_wait(terminator);
  if (rc != 0)
  {
    return rc;
  }

  PRINTDBG("Shutting down workers\n");
  rc = shutdown_workers();
  if (rc != 0)
  {
    return rc;
  }

  Terminator_free(terminator);

  PRINTDBG("done waiting\n");

  ex = (BehaviorException *)atomic_load_ptr(&behavior_exceptions);
  if (ex != NULL)
  {
    while (ex != NULL)
    {
      BehaviorException *next = ex->next;
      if (ex->msg != NULL)
      {
        PyErr_Format(WhenError, "%s: %s", ex->name, ex->msg);
      }
      else
      {
        PyErr_SetString(WhenError, ex->name);
      }
      BehaviorException_free(ex);
      ex = next;
    }

    free_subinterpreters();
    return -1;
  }

  free_subinterpreters();

  ht_free(global_imm_object_regions);

  return 0;
}

static PyObject *veronapy_run(PyObject *veronapymodule, PyObject *Py_UNUSED(ignored))
{
  if (VPY_run() != 0)
  {
    return NULL;
  }

  Py_RETURN_NONE;
}

static PyObject *veronapy_wait(PyObject *veronapymodule, PyObject *Py_UNUSED(ignored))
{
  if (VPY_wait() != 0)
  {
    return NULL;
  }

  Py_RETURN_NONE;
}

static PyObject *veronapy_workercount(PyObject *veronapymodule, PyObject *Py_UNUSED(ignored))
{
  return PyLong_FromSsize_t(worker_count);
}

static PyMethodDef veronapy_methods[] = {
    {"when", when, METH_VARARGS, "when decorator"},
    {"wait", (PyCFunction)veronapy_wait, METH_NOARGS, "wait for all behaviors to complete"},
    {"run", (PyCFunction)veronapy_run, METH_NOARGS, "start the runtime."},
    {"worker_count", (PyCFunction)veronapy_workercount, METH_NOARGS, "get the number of workers."},
    {NULL} /* Sentinel */
};

static int veronapy_exec(PyObject *module)
{
  PyTypeObject *region_type, *merge_type, *when_type, *regiontag_type;

  region_type = &RegionType;
  if (PyType_Ready(region_type) < 0)
  {
    return -1;
  }

  merge_type = &MergeType;
  if (PyType_Ready(merge_type) < 0)
  {
    return -1;
  }

  when_type = &WhenType;
  if (PyType_Ready(when_type) < 0)
  {
    return -1;
  }

  regiontag_type = &RegionTagType;
  if (PyType_Ready(regiontag_type) < 0)
  {
    return -1;
  }

  PyModule_AddStringConstant(module, "__version__", "0.0.3");
  RegionIsolationError = PyErr_NewException("veronapy.RegionIsolationError", NULL, NULL);
  Py_XINCREF(RegionIsolationError);
  if (PyModule_AddObject(module, "RegionIsolationError", RegionIsolationError) < 0)
  {
    Py_XDECREF(RegionIsolationError);
    Py_CLEAR(RegionIsolationError);
    return -1;
  }

  WhenError = PyErr_NewException("veronapy.WhenError", NULL, NULL);
  Py_XINCREF(WhenError);
  if (PyModule_AddObject(module, "WhenError", WhenError) < 0)
  {
    Py_XDECREF(WhenError);
    Py_CLEAR(WhenError);
    return -1;
  }

  Py_INCREF(region_type);
  if (PyModule_AddObject(module, "region", (PyObject *)region_type) <
      0)
  {
    Py_DECREF(region_type);
    return -1;
  }

  Py_INCREF(merge_type);
  if (PyModule_AddObject(module, "merge", (PyObject *)merge_type) < 0)
  {
    Py_DECREF(merge_type);
    return -1;
  }

  Py_INCREF(when_type);
  if (PyModule_AddObject(module, "when_factory", (PyObject *)when_type) < 0)
  {
    Py_DECREF(when_type);
    return -1;
  }

  Py_INCREF(regiontag_type);
  if (PyModule_AddObject(module, "regiontag", (PyObject *)regiontag_type) < 0)
  {
    Py_DECREF(regiontag_type);
    return -1;
  }

  vpy_state = (VPYState *)PyModule_GetState(module);
  vpy_state->isolated_types = PyDict_New();
  if (vpy_state->isolated_types == NULL)
  {
    return -1;
  }

  vpy_state->imm_object_regions = PyDict_New();
  if (vpy_state->imm_object_regions == NULL)
  {
    return -1;
  }

  vpy_state->frozen_types = PyDict_New();
  if (vpy_state->frozen_types == NULL)
  {
    return -1;
  }

  if (alloc_id == 0)
  {
    return VPY_run();
  }

  return 0;
}

void veronapy_free(PyObject *module)
{
  VPYState *state = (VPYState *)PyModule_GetState(module);
  if (state != NULL)
  {
    Py_XDECREF(state->isolated_types);
    Py_XDECREF(state->imm_object_regions);
  }
}

#ifdef Py_mod_exec
static PyModuleDef_Slot veronapy_slots[] = {
    {Py_mod_exec, (void *)veronapy_exec},
#ifdef VPY_MULTIGIL
    {Py_mod_multiple_interpreters, Py_MOD_PER_INTERPRETER_GIL_SUPPORTED},
#endif
    {0, NULL},
};
#endif

static PyModuleDef veronapymoduledef = {
    PyModuleDef_HEAD_INIT,
    .m_name = "veronapy",
    .m_doc = "veronapy is a Python extension that adds Behavior-oriented "
             "Concurrency runtime for Python.",
    .m_methods = veronapy_methods,
    .m_free = (freefunc)veronapy_free,
#ifdef Py_mod_exec
    .m_slots = veronapy_slots,
#endif
    .m_size = sizeof(VPYState)};

PyMODINIT_FUNC PyInit_veronapy(void)
{
#ifdef Py_mod_exec
  return PyModuleDef_Init(&veronapymoduledef);
#else
  PyObject *module;
  module = PyModule_Create(&veronapymoduledef);
  if (module == NULL)
    return NULL;

  if (veronapy_exec(module) != 0)
  {
    Py_DECREF(module);
    return NULL;
  }

  return module;
#endif
}