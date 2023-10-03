#define PY_SSIZE_T_CLEAN
#include <Python.h>

#ifdef _WIN32
#include <windows.h>
typedef unsigned long long atomic_ullong;

atomic_ullong atomic_fetch_add(atomic_ullong *ptr, atomic_ullong val)
{
  return InterlockedExchangeAdd64(ptr, val);
}
#else
#include <stdatomic.h>
#endif

/***************************************************************/
/*                     Macros and Defines                      */
/***************************************************************/

#if PY_VERSION_HEX < 0x030C0000 // Python 3.12
#define PyType_GetDict(t) ((t)->tp_dict)
#endif

typedef long bool;
#define true 1
#define false 0

#define _VPY_GETTYPE(n, i, r)                                                  \
  n = (PyTypeObject *)PyDict_GetItemString(PyType_GetDict(i), "__isolated__"); \
  if (n == NULL)                                                               \
  {                                                                            \
    PyErr_SetString(PyExc_TypeError,                                           \
                    "error obtaining internal type of isolated object");       \
    return r;                                                                  \
  }

#define VPY_GETTYPE(r) \
  PyTypeObject *type;  \
  _VPY_GETTYPE(type, isolated_type, r)

#define _VPY_GETREGION(n, r)                                              \
  n = (RegionObject *)PyDict_GetItemString(PyType_GetDict(isolated_type), \
                                           "__region__");                 \
  if (n == NULL)                                                          \
  {                                                                       \
    PyErr_SetString(PyExc_TypeError,                                      \
                    "error obtaining region of isolated object");         \
    return r;                                                             \
  }

#define VPY_GETREGION(r) \
  RegionObject *region;  \
  _VPY_GETREGION(region, r)

#define VPY_CHECKREGIONOPEN(r)                                 \
  if (!region->is_open)                                        \
  {                                                            \
    PyErr_SetString(PyExc_RuntimeError, "Region is not open"); \
    return r;                                                  \
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
    PyErr_SetString(PyExc_RuntimeError,                          \
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
    PyErr_SetString(PyExc_RuntimeError,                           \
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

/***************************************************************/
/*                     Useful functions                        */
/***************************************************************/

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

/***************************************************************/
/*                     Module variables                        */
/***************************************************************/

static PyModuleDef veronapymoduledef = {
    PyModuleDef_HEAD_INIT,
    .m_name = "veronapy",
    .m_doc = "veronapy is a Python extension that adds Behavior-oriented "
             "Concurrency runtime for Python.",
    .m_size = 0,
};

atomic_ullong region_identity = 0;

/***************************************************************/
/*              Region struct and functions                    */
/***************************************************************/

typedef struct
{
  PyObject_HEAD
      PyObject *name;
  PyObject *alias;
  unsigned long long id;
  bool is_open;
  bool is_shared;
  PyObject *parent;
  PyObject *objects;
  PyObject *types;
} RegionObject;

static const char *REGION_ATTRS[] = {
    "name",
    "id",
    "parent",
    "is_open",
    "merge",
    "is_shared",
    "__enter__",
    "__exit__",
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

// The region of an object (or an implicit region if the object is not
// isolated)
static RegionObject *region_of(PyObject *value)
{
  PyTypeObject *isolated_type = Py_TYPE(value);
  RegionObject *region = (RegionObject *)PyDict_GetItemString(
      PyType_GetDict(isolated_type), "__region__");
  if (region == NULL)
  {
    return NULL;
  }

  if (region->alias != (PyObject *)region)
  {
    region = resolve_region(region);
    if (PyDict_SetItemString(PyType_GetDict(isolated_type), "__region__", (PyObject *)region) < 0)
    {
      PyErr_WarnEx(PyExc_RuntimeWarning, "Unable to set region of object", 1);
    }
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

static PyTypeObject *isolate_type(RegionObject *region, PyTypeObject *type);

// Attempts to capture an object into the given region.
// This function will find all the types of all the objects
// in the graph for which value is the root and capture them into the
// region. If they have already been captured, it will use the type it
// previously captured.
static int capture_object(RegionObject *region, PyObject *value)
{
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
    PyErr_SetString(PyExc_RuntimeError,
                    "Object already captured by another region");
    return -1;
  }

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
      PyErr_SetString(PyExc_RuntimeError,
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
      (PyTypeObject *)PyDict_GetItem(region->types, (PyObject *)type);
  if (isolated_type == NULL)
  {
    isolated_type = isolate_type(region, type);
    if (isolated_type == NULL)
    {
      return -1;
    }

    rc = PyDict_SetItem(region->types, (PyObject *)type,
                        (PyObject *)isolated_type);
    if (rc < 0)
    {
      PyErr_SetString(PyExc_RuntimeError,
                      "Unable to add isolated type to region");
      return -1;
    }
  }

  Py_INCREF((PyObject *)isolated_type);
  value->ob_type = isolated_type;

  return rc;
}

/***************************************************************/
/*              Isolated object methods                        */
/***************************************************************/

static void Isolated_finalize(PyObject *self)
{
  PyTypeObject *isolated_type = Py_TYPE(self);
  PyTypeObject *type = (PyTypeObject *)PyDict_GetItemString(PyType_GetDict(isolated_type), "__isolated__");
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

static int Isolated_traverse(PyObject *self, visitproc visit, void *arg)
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
}

static PyObject *Isolated_getattro(PyObject *self, PyObject *attr_name)
{
  PyTypeObject *isolated_type = Py_TYPE(self);
  VPY_GETTYPE(NULL);
  VPY_GETREGION(NULL);

  if (strcmp(PyUnicode_AsUTF8(attr_name), "__region__") == 0)
  {
    Py_INCREF(region);
    return (PyObject *)region;
  }

  if (PyObject_HasAttr((PyObject *)isolated_type, attr_name))
  {
    return PyObject_GenericGetAttr(self, attr_name);
  }

  if (region->is_open)
  {
    self->ob_type = type;
    PyObject *obj = PyObject_GenericGetAttr(self, attr_name);
    self->ob_type = isolated_type;
    return obj;
  }

  PyErr_SetString(PyExc_RuntimeError, "Region is not open");
  return NULL;
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
    PyErr_SetString(PyExc_RuntimeError, "Region is not open");
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

// Creates a new isolated type for the given type
// The new type will wrap all of the methods of the given type
// to enforce that the region containing an object of this
// type is open. The resulting type will wrap a new type object
// that is a copy of the given type object.
static PyTypeObject *isolate_type(RegionObject *region, PyTypeObject *type)
{
  PyType_Slot slots[] = {
      {Py_tp_repr, NULL},
      {Py_tp_str, NULL},
      {Py_tp_traverse, NULL},
      {Py_tp_clear, NULL},
      {Py_tp_hash, NULL},
      {Py_tp_iter, NULL},
      {Py_tp_iternext, NULL},
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

  const int num_tp_slots = 7;
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
    slots[2].pfunc = Isolated_traverse;
  }
  if (type->tp_clear != NULL)
  {
    slots[3].pfunc = Isolated_clear;
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

  PyType_Spec spec = {
      .name = "region.isolated",
      .basicsize = (int)type->tp_basicsize,
      .itemsize = (int)type->tp_itemsize,
      .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HEAPTYPE,
      .slots = slots,
  };

  PyTypeObject *isolated_type = (PyTypeObject *)PyType_FromSpec(&spec);
  if (PyDict_SetItemString(PyType_GetDict(isolated_type), "__isolated__",
                           (PyObject *)type) < 0)
  {
    Py_DECREF((PyObject *)isolated_type);
    PyErr_SetString(PyExc_TypeError,
                    "error adding internal type to isolated type dictionary");
    return NULL;
  }

  Py_INCREF((PyObject *)region);
  if (PyDict_SetItemString(PyType_GetDict(isolated_type), "__region__",
                           (PyObject *)region) < 0)
  {
    Py_DECREF((PyObject *)region);
    Py_DECREF((PyObject *)isolated_type);
    PyErr_SetString(PyExc_TypeError,
                    "error adding region to isolated type dictionary");
    return NULL;
  }

  type->tp_flags = Py_TPFLAGS_IMMUTABLETYPE | type->tp_flags;
  return isolated_type;
}

/***************************************************************/
/*              Merge struct and methods                       */
/***************************************************************/

typedef struct
{
  PyObject_HEAD
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

static int Region_clear(RegionObject *self)
{
  Py_CLEAR(self->objects);
  Py_CLEAR(self->types);
  Py_CLEAR(self->alias);
  Py_CLEAR(self->parent);
  return 0;
}

static void Region_dealloc(RegionObject *self)
{
  PyObject_GC_UnTrack(self);
  Region_clear(self);
  Py_XDECREF(self->name);
  Py_TYPE(self)->tp_free((PyObject *)self);
}

static int Region_traverse(RegionObject *self, visitproc visit, void *arg)
{
  Py_VISIT(self->objects);
  Py_VISIT(self->types);
  Py_VISIT(self->alias);
  Py_VISIT(self->parent);
  return 0;
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
    self->types = PyDict_New();
    if (self->types == NULL)
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
  PyObject *name = NULL, *tmp;

  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|O", kwlist, &name))
    return -1;

  self->id = atomic_fetch_add(&region_identity, 1);

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

static bool Region_Check(PyObject *obj);

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
    PyErr_SetString(PyExc_RuntimeError, "Region is not open");
    return NULL;
  }

  if (!is_free(region))
  {
    PyErr_SetString(PyExc_RuntimeError, "Region is not free");
    return NULL;
  }

  other->alias = (PyObject *)region;

  PyObject *argList = Py_BuildValue("(O)", other->objects);

  PyObject *merged = PyObject_Call((PyObject *)&MergeType, argList, NULL);
  if (merged == NULL)
  {
    return NULL;
  }

  Py_DECREF(argList);
  Py_INCREF(merged);
  return merged;
}

static PyObject *Region_str(RegionObject *self, PyObject *Py_UNUSED(ignored))
{
  VPY_REGION(self);
  return PyUnicode_FromFormat("Region(%S<%d>)", region->name, region->id);
}

static PyObject *Region_repr(RegionObject *self, PyObject *Py_UNUSED(ignored))
{
  VPY_REGION(self);
  return PyUnicode_FromFormat("Region(%S<%d>%s%s)", region->name,
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
    PyErr_SetString(PyExc_RuntimeError, "Region is not open");
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
    PyErr_SetString(PyExc_RuntimeError, "Region is not open");
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

  PyErr_SetString(PyExc_RuntimeError, "Value belongs to another region");
  return -1;
}

static PyTypeObject RegionType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "veronapy.region",
    .tp_doc = PyDoc_STR("Region object"),
    .tp_basicsize = sizeof(RegionObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_new = Region_new,
    .tp_init = (initproc)Region_init,
    .tp_dealloc = (destructor)Region_dealloc,
    .tp_methods = Region_methods,
    .tp_getset = Region_getsetters,
    .tp_repr = (reprfunc)Region_repr,
    .tp_str = (reprfunc)Region_str,
    .tp_setattro = (setattrofunc)Region_setattro,
    .tp_getattro = (getattrofunc)Region_getattro,
    .tp_traverse = (traverseproc)Region_traverse,
    .tp_clear = (inquiry)Region_clear,
};

static bool Region_Check(PyObject *obj)
{
  PyTypeObject *obj_type = Py_TYPE(obj);
  PyTypeObject *region_type = (PyTypeObject *)&RegionType;
  return obj_type == region_type;
}

/***************************************************************/
/*                  Module setup                               */
/***************************************************************/

PyMODINIT_FUNC PyInit_veronapy(void)
{
  PyObject *veronapymodule;
  PyTypeObject *region_type, *merge_type;

  region_type = &RegionType;
  if (PyType_Ready(region_type) < 0)
    return NULL;

  merge_type = &MergeType;
  if (PyType_Ready(merge_type) < 0)
    return NULL;

  veronapymodule = PyModule_Create(&veronapymoduledef);
  if (veronapymodule == NULL)
    return NULL;

  PyModule_AddStringConstant(veronapymodule, "__version__", "0.0.2");

  Py_INCREF(&RegionType);
  if (PyModule_AddObject(veronapymodule, "region", (PyObject *)region_type) <
      0)
  {
    Py_DECREF(region_type);
    Py_DECREF(veronapymodule);
    return NULL;
  }

  Py_INCREF(merge_type);
  if (PyModule_AddObject(veronapymodule, "merge", (PyObject *)merge_type) < 0)
  {
    Py_DECREF(merge_type);
    Py_DECREF(veronapymodule);
    return NULL;
  }

  return veronapymodule;
}