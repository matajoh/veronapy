#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "structmember.h"
#include <stdatomic.h>

atomic_ullong region_identity = 0;

typedef long bool;
#define true 1
#define false 0

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
  PyTypeObject *type = (PyTypeObject *)PyDict_GetItemString(
      isolated_type->tp_dict, "__isolated__");
  if (isolated_type == NULL)
  {
    printf("error getting __isolated__");
    return;
  }

  self->ob_type = type;
  Py_DECREF(isolated_type);
}

static PyObject *Isolated_repr(PyObject *self)
{
  PyTypeObject *isolated_type = Py_TYPE(self);
  PyTypeObject *type = (PyTypeObject *)PyDict_GetItemString(
      isolated_type->tp_dict, "__isolated__");
  if (isolated_type == NULL)
  {
    printf("error getting __isolated__");
    return NULL;
  }

  RegionObject *region = (RegionObject *)PyDict_GetItemString(
      isolated_type->tp_dict, "__region__");
  if (region == NULL)
  {
    printf("error getting __region__");
    return NULL;
  }

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
  PyTypeObject *type = (PyTypeObject *)PyDict_GetItemString(
      isolated_type->tp_dict, "__isolated__");
  if (isolated_type == NULL)
  {
    printf("error getting __isolated__");
    return -1;
  }

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
  PyTypeObject *type = (PyTypeObject *)PyDict_GetItemString(
      isolated_type->tp_dict, "__isolated__");
  if (isolated_type == NULL)
  {
    printf("error getting __isolated__");
    return -1;
  }

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
  PyTypeObject *type = (PyTypeObject *)PyDict_GetItemString(
      isolated_type->tp_dict, "__isolated__");
  if (isolated_type == NULL)
  {
    printf("error getting __isolated__");
    return NULL;
  }

  RegionObject *region = (RegionObject *)PyDict_GetItemString(
      isolated_type->tp_dict, "__region__");
  if (region == NULL)
  {
    printf("error getting __region__");
    return NULL;
  }

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
  PyTypeObject *type = Py_TYPE(value);
  RegionObject *region = (RegionObject *)PyDict_GetItemString(
      type->tp_dict, "__region__");
  if (region == NULL)
  {
    return implicit_region;
  }

  return region;
}

static int capture_object(RegionObject *region, PyObject *value);

static int Isolated_setattro(PyObject *self, PyObject *attr_name,
                             PyObject *value)
{
  int rc;

  PyTypeObject *isolated_type = Py_TYPE(self);
  PyTypeObject *type = (PyTypeObject *)PyDict_GetItemString(
      isolated_type->tp_dict, "__isolated__");
  if (isolated_type == NULL)
  {
    printf("error getting __isolated__");
    return -1;
  }

  RegionObject *region = (RegionObject *)PyDict_GetItemString(
      isolated_type->tp_dict, "__region__");
  if (region == NULL)
  {
    printf("error getting __region__");
    return -1;
  }

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

  RegionObject *value_region = region_of(value);
  if (value_region == implicit_region)
  {
    capture_object(region, value);
    value_region = region;
  }

  if (value_region == region)
  {
    self->ob_type = type;
    rc = PyObject_GenericSetAttr(self, attr_name, value);
    self->ob_type = isolated_type;
    return rc;
  }

  PyErr_SetString(PyExc_RuntimeError, "Value belongs to another region");
  return -1;
}

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
      printf("Unable to enumerate over immutable sequence");
      return false;
    }

    len = PySequence_Length(seq);
    for (i = 0; i < len; i++)
    {
      PyObject *item = PySequence_GetItem(seq, i);
      if (item == NULL)
      {
        printf("Unable to get item from sequence");
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
        {Py_tp_finalize, Isolated_finalize},
        {Py_tp_repr, Isolated_repr},
        {Py_tp_traverse, Isolated_traverse},
        {Py_tp_clear, Isolated_clear},
        {Py_tp_getattro, Isolated_getattro},
        {Py_tp_setattro, Isolated_setattro},
        {0, NULL} /* Sentinel */
    };

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
      printf("error creating isolated type");
      return -1;
    }

    rc = PyDict_SetItemString(isolated_type->tp_dict, "__isolated__",
                              (PyObject *)type);
    if (rc < 0)
    {
      printf("error setting __isolated__");
      return rc;
    }

    rc = PyDict_SetItemString(isolated_type->tp_dict, "__region__",
                              (PyObject *)region);
    if (rc < 0)
    {
      printf("error setting __region__");
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