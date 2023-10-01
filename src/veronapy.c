#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "structmember.h"
#include <stdatomic.h>

atomic_ullong region_identity = 0;

typedef long bool;
#define true 1
#define false 0

#define _VPY_GETTYPE(n, i, r)                                                             \
  n = (PyTypeObject *)PyDict_GetItemString(i->tp_dict, "__isolated__");                   \
  if (n == NULL)                                                                          \
  {                                                                                       \
    PyErr_SetString(PyExc_TypeError, "error obtaining internal type of isolated object"); \
    return r;                                                                             \
  }

#define VPY_GETTYPE(r) \
  PyTypeObject *type;  \
  _VPY_GETTYPE(type, isolated_type, r)

#define _VPY_GETREGION(n, r)                                                       \
  n = (RegionObject *)PyDict_GetItemString(isolated_type->tp_dict, "__region__");  \
  if (n == NULL)                                                                   \
  {                                                                                \
    PyErr_SetString(PyExc_TypeError, "error obtaining region of isolated object"); \
    return r;                                                                      \
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

#define VPY_CHECKARG0REGION(r)                                                       \
  PyTypeObject *arg0_type = Py_TYPE(arg0);                                           \
  PyTypeObject *arg0_isolated_type = arg0_type;                                      \
  RegionObject *arg0_region = region_of(arg0);                                       \
  if (arg0_region == implicit_region)                                                \
  {                                                                                  \
    arg0_type = arg0_isolated_type;                                                  \
    capture_object(region, arg0);                                                    \
    arg0_region = region;                                                            \
    arg0_isolated_type = Py_TYPE(arg0);                                              \
  }                                                                                  \
  else if (arg0_region == region)                                                    \
  {                                                                                  \
    _VPY_GETTYPE(arg0_type, arg0_isolated_type, r);                                  \
  }                                                                                  \
  else                                                                               \
  {                                                                                  \
    PyErr_SetString(PyExc_RuntimeError, "First argument belongs to another region"); \
    return r;                                                                        \
  }

#define VPY_CHECKARG1REGION(r)                                                        \
  PyTypeObject *arg1_type = Py_TYPE(arg1);                                            \
  PyTypeObject *arg1_isolated_type = arg1_type;                                       \
  RegionObject *arg1_region = region_of(arg1);                                        \
  if (arg1_region == implicit_region)                                                 \
  {                                                                                   \
    arg1_type = arg1_isolated_type;                                                   \
    capture_object(region, arg1);                                                     \
    arg1_region = region;                                                             \
    arg1_isolated_type = Py_TYPE(arg1);                                               \
  }                                                                                   \
  else if (arg1_region == region)                                                     \
  {                                                                                   \
    _VPY_GETTYPE(arg1_type, arg1_isolated_type, r);                                   \
  }                                                                                   \
  else                                                                                \
  {                                                                                   \
    PyErr_SetString(PyExc_RuntimeError, "Second argument belongs to another region"); \
    return r;                                                                         \
  }

static const char *REGION_ATTRS[] = {
    "name",
    "__identity__",
    "__region__",
    "is_open",
    "is_shared",
    "__enter__",
    "__exit__",
    NULL,
};

static PyModuleDef veronapymoduledef = {
    PyModuleDef_HEAD_INIT,
    .m_name = "veronapy",
    .m_doc = "veronapy is a Python extension that adds Behavior-oriented "
             "Concurrency runtime for Python.",
    .m_size = -1,
};

static PyObject *veronapymodule = NULL;

////// Region //////

typedef struct
{
  PyObject_HEAD;
  PyObject *name;
  PyObject *alias;
  unsigned long long __identity__;
  bool is_open;
  bool is_shared;
  PyObject *__region__;
  PyObject *objects;
} RegionObject;

static RegionObject *implicit_region = NULL;

static void Region_dealloc(RegionObject *self)
{
  Py_XDECREF(self->name);
  Py_XDECREF(self->alias);
  Py_XDECREF(self->__region__);
  Py_DECREF(self->objects);
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
    Py_INCREF(Py_None);
    self->alias = Py_None;
    Py_INCREF(Py_None);
    self->__region__ = Py_None;
    self->is_open = false;
    self->is_shared = false;
    self->__identity__ = 0;
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
  PyObject *name = NULL, *tmp;

  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|O", kwlist, &name))
    return -1;

  self->__identity__ = atomic_fetch_add(&region_identity, 1);

  if (name)
  {
    tmp = self->name;
    Py_INCREF(name);
    self->name = name;
    Py_XDECREF(tmp);
  }
  else
  {
    self->name = PyUnicode_FromFormat("region_%llu", self->__identity__);
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
  Py_INCREF(self->name);
  return self->name;
}

static int Region_setname(RegionObject *self, PyObject *value, void *closure)
{
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

  tmp = self->name;
  Py_INCREF(value);
  self->name = value;
  Py_XDECREF(tmp);
  return 0;
}

static PyObject *Region_getidentity(RegionObject *self, void *closure)
{
  return PyLong_FromUnsignedLongLong(self->__identity__);
}

static int Region_setidentity(RegionObject *self, PyObject *value,
                              void *closure)
{
  PyErr_SetString(PyExc_TypeError, "Cannot set the __identity__ attribute");
  return -1;
}

static PyObject *Region_getregion(RegionObject *self, void *closure)
{
  Py_INCREF(self->__region__);
  return self->__region__;
}

static int Region_setregion(RegionObject *self, PyObject *value,
                            void *closure)
{
  PyErr_SetString(PyExc_TypeError, "Cannot set the __region__ attribute");
  return -1;
}

static PyObject *Region_getisopen(RegionObject *self, void *closure)
{
  return PyBool_FromLong(self->is_open);
}

static int Rego_setisopen(RegionObject *self, PyObject *value, void *closure)
{
  PyErr_SetString(PyExc_TypeError, "Cannot set the is_open attribute");
  return -1;
}

static PyObject *Region_getisshared(RegionObject *self, void *closure)
{
  return PyBool_FromLong(self->is_shared);
}

static int Region_setisshared(RegionObject *self, PyObject *value,
                              void *closure)
{
  PyErr_SetString(PyExc_TypeError, "Cannot set the is_shared attribute");
  return -1;
}

static PyGetSetDef Region_getsetters[] = {
    {"name", (getter)Region_getname, (setter)Region_setname,
     "Human-readable name for the region", NULL},
    {"__identity__", (getter)Region_getidentity, (setter)Region_setidentity,
     "The immutable, unique identity for the region", NULL},
    {"__region__", (getter)Region_getregion, (setter)Region_setregion,
     "The parent region", NULL},
    {"is_open", (getter)Region_getisopen, (setter)Rego_setisopen,
     "Whether the region is currently open", NULL},
    {"is_shared", (getter)Region_getisshared, (setter)Region_setisshared,
     "Whether the region is shared", NULL},
    {NULL} /* Sentinel */
};

static PyObject *Region_str(RegionObject *self, PyObject *Py_UNUSED(ignored))
{
  return PyUnicode_FromFormat("Region(%S<%d>)", self->name, self->__identity__);
}

static PyObject *Region_enter(RegionObject *self,
                              PyObject *Py_UNUSED(ignored))
{
  self->is_open = true;
  Py_INCREF((PyObject *)self);
  return (PyObject *)self;
}

static PyObject *Region_exit(RegionObject *self, PyObject *args,
                             PyObject *kwds)
{
  PyObject *type, *value, *traceback;
  if (!PyArg_ParseTuple(args, "OOO", &type, &value, &traceback))
    return NULL;

  if (type == Py_None)
  {
    Py_XDECREF(type);
    Py_XDECREF(value);
    Py_XDECREF(traceback);
    self->is_open = false;
  }
  else
  {
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
    {NULL} /* Sentinel */
};

////// Isolated //////

static void Isolated_finalize(PyObject *self)
{
  PyTypeObject *isolated_type = Py_TYPE(self);
  VPY_GETTYPE();
  self->ob_type = type;
  Py_DECREF(isolated_type);
}

static PyObject *Isolated_repr(PyObject *self)
{
  PyTypeObject *isolated_type = Py_TYPE(self);
  VPY_GETTYPE(NULL);
  VPY_GETREGION(NULL);

  if (region->is_open)
  {
    self->ob_type = type;
    PyObject *repr = PyObject_Repr(self);
    self->ob_type = isolated_type;
    return repr;
  }

  PyErr_SetString(PyExc_RuntimeError, "Region is not open");
  return NULL;
}

static int Isolated_traverse(PyObject *self, visitproc visit, void *arg)
{
  int rc;
  PyTypeObject *isolated_type = Py_TYPE(self);
  VPY_GETTYPE(-1);

  traverseproc tp_traverse = type->tp_traverse;
  if (tp_traverse != NULL)
  {
    self->ob_type = type;
    rc = tp_traverse(self, visit, arg);
    self->ob_type = isolated_type;
    return rc;
  }

  return 0;
}

static int Isolated_clear(PyObject *self)
{
  int rc;

  PyTypeObject *isolated_type = Py_TYPE(self);
  VPY_GETTYPE(-1);

  inquiry tp_clear = type->tp_clear;
  if (tp_clear != NULL)
  {
    self->ob_type = type;
    rc = tp_clear(self);
    self->ob_type = isolated_type;
    return rc;
  }

  return 0;
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

static RegionObject *region_of(PyObject *value)
{
  PyTypeObject *isolated_type = Py_TYPE(value);
  RegionObject *region = (RegionObject *)PyDict_GetItemString(isolated_type->tp_dict, "__region__");
  if (region == NULL)
  {
    return implicit_region;
  }

  return region;
}

static int capture_object(RegionObject *region, PyObject *value);

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

#define VPY_OBJOBJARGPROC(interface, name)            \
  int Isolated_##name(PyObject *self, PyObject *arg0, \
                      PyObject *arg1)                 \
  {                                                   \
    PyTypeObject *isolated_type = Py_TYPE(self);      \
    int rc;                                           \
    VPY_GETTYPE(-1);                                  \
    VPY_GETREGION(-1);                                \
    VPY_CHECKREGIONOPEN(-1);                          \
    VPY_CHECKARG0REGION(-1);                          \
    if (arg1 != NULL)                                 \
    {                                                 \
      VPY_CHECKARG1REGION(-1);                        \
    }                                                 \
    self->ob_type = type;                             \
    rc = type->interface->name(self, arg0, arg1);     \
    self->ob_type = isolated_type;                    \
    return rc;                                        \
  }

#define VPY_SSIZEARGFUNC(interface, name)          \
  PyObject *Isolated_##name(PyObject *self,        \
                            Py_ssize_t arg)        \
  {                                                \
    PyTypeObject *isolated_type = Py_TYPE(self);   \
    VPY_GETTYPE(NULL);                             \
    VPY_GETREGION(NULL);                           \
    VPY_CHECKREGIONOPEN(NULL);                     \
    self->ob_type = type;                          \
    PyObject *result = type->interface->name(self, \
                                             arg); \
    self->ob_type = isolated_type;                 \
    return result;                                 \
  }

#define VPY_SSIZEOBJARGPROC(interface, name)           \
  int Isolated_##name(PyObject *self,                  \
                      Py_ssize_t arg0, PyObject *arg1) \
  {                                                    \
    PyTypeObject *isolated_type = Py_TYPE(self);       \
    int rc;                                            \
    VPY_GETTYPE(-1);                                   \
    VPY_GETREGION(-1);                                 \
    VPY_CHECKREGIONOPEN(-1);                           \
    if (arg1 != NULL)                                  \
    {                                                  \
      VPY_CHECKARG1REGION(-1);                         \
    }                                                  \
    self->ob_type = type;                              \
    rc = type->interface->name(self, arg0, arg1);      \
    self->ob_type = isolated_type;                     \
    return rc;                                         \
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

static bool is_imm(PyObject *value)
{
  if (Py_IsNone(value) || PyBool_Check(value) || PyLong_Check(value) || PyFloat_Check(value) || PyComplex_Check(value) || PyUnicode_Check(value) || PyBytes_Check(value) || PyRange_Check(value))
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

static int capture_object(RegionObject *region, PyObject *value)
{
  int rc = 0;
  PyTypeObject *type = Py_TYPE(value);
  if (!is_imm(value))
  {
    PyType_Slot slots[] = {
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
        {Py_tp_repr, Isolated_repr},
        {Py_tp_traverse, Isolated_traverse},
        {Py_tp_clear, Isolated_clear},
        {Py_tp_getattro, Isolated_getattro},
        {Py_tp_setattro, Isolated_setattro},
        {0, NULL} /* Sentinel */
    };

    const int num_mp_slots = 3;
    const int num_sq_slots = 8;

    if (type->tp_as_mapping != NULL)
    {
      PyMappingMethods *mp_methods = type->tp_as_mapping;
      PyType_Slot *mp_slots = slots + 0;
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
      PyType_Slot *sq_slots = slots + num_mp_slots;
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
      PyType_Slot *nb_slots = slots + num_mp_slots + num_sq_slots;
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
        .basicsize = type->tp_basicsize,
        .itemsize = type->tp_itemsize,
        .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HEAPTYPE,
        .slots = slots,
    };

    PyTypeObject *isolated_type = (PyTypeObject *)PyType_FromModuleAndSpec(veronapymodule, &spec, NULL);
    if (isolated_type == NULL)
    {
      PyErr_SetString(PyExc_TypeError, "error creating isolated type");
      return -1;
    }

    rc = PyDict_SetItemString(isolated_type->tp_dict, "__isolated__",
                              (PyObject *)type);
    if (rc < 0)
    {
      PyErr_SetString(PyExc_TypeError, "error adding internal type to isolated type dictionary");
      return rc;
    }

    rc = PyDict_SetItemString(isolated_type->tp_dict, "__region__",
                              (PyObject *)region);
    if (rc < 0)
    {
      PyErr_SetString(PyExc_TypeError, "error adding region to isolated type dictionary");
      return rc;
    }

    Py_INCREF((PyObject *)isolated_type);
    value->ob_type = isolated_type;
  }

  return rc;
}

static PyObject *Region_getattro(RegionObject *self, PyObject *attr_name)
{
  for (int i = 0; REGION_ATTRS[i] != NULL; i++)
  {
    if (strcmp(PyUnicode_AsUTF8(attr_name), REGION_ATTRS[i]) == 0)
    {
      return PyObject_GenericGetAttr((PyObject *)self, attr_name);
    }
  }

  if (!self->is_open)
  {
    PyErr_SetString(PyExc_RuntimeError, "Region is not open");
    return NULL;
  }

  PyObject *obj = PyDict_GetItemWithError(self->objects, attr_name);
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
  for (int i = 0; REGION_ATTRS[i] != NULL; i++)
  {
    if (strcmp(PyUnicode_AsUTF8(attr_name), REGION_ATTRS[i]) == 0)
    {
      return PyObject_GenericSetAttr((PyObject *)self, attr_name, value);
    }
  }

  if (!self->is_open)
  {
    PyErr_SetString(PyExc_RuntimeError, "Region is not open");
    return -1;
  }

  RegionObject *value_region = region_of(value);
  if (value_region == implicit_region)
  {
    capture_object(self, value);
    value_region = self;
  }

  if (value_region == self)
  {
    Py_INCREF(value);
    rc = PyDict_SetItem(self->objects, attr_name, value);
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
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = Region_new,
    .tp_init = (initproc)Region_init,
    .tp_dealloc = (destructor)Region_dealloc,
    .tp_methods = Region_methods,
    .tp_getset = Region_getsetters,
    .tp_str = (reprfunc)Region_str,
    .tp_setattro = (setattrofunc)Region_setattro,
    .tp_getattro = (getattrofunc)Region_getattro,
};

PyMODINIT_FUNC PyInit_veronapy(void)
{
  if (PyType_Ready(&RegionType) < 0)
    return NULL;

  veronapymodule = PyModule_Create(&veronapymoduledef);
  if (veronapymodule == NULL)
    return NULL;

  PyModule_AddStringConstant(veronapymodule, "__version__", "0.0.1");

  Py_INCREF(&RegionType);
  if (PyModule_AddObject(veronapymodule, "region", (PyObject *)&RegionType) < 0)
  {
    Py_DECREF(&RegionType);
    Py_DECREF(veronapymodule);
    return NULL;
  }

  /* Pass two arguments, a string and an int. */
  PyObject *argList = Py_BuildValue("(s)", "__implicit__");

  /* Call the class object. */
  implicit_region = (RegionObject *)PyObject_CallObject((PyObject *)&RegionType, argList);
  if (implicit_region == NULL)
  {
    Py_DECREF(argList);
    Py_DECREF(&RegionType);
    Py_DECREF(veronapymodule);
    return NULL;
  }

  /* Release the argument list. */
  Py_DECREF(argList);

  return veronapymodule;
}