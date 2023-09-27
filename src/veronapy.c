#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "structmember.h"
#include <stdatomic.h>

atomic_ullong region_identity = 1;

typedef long bool;
#define true 1
#define false 0

typedef struct {
  PyObject_HEAD PyObject *name;
  PyObject *alias;
  unsigned long long __identity__;
  bool is_open;
  bool is_shared;
  PyObject *__region__;
} RegionObject;

static void Region_dealloc(RegionObject *self) {
  Py_XDECREF(self->name);
  Py_XDECREF(self->alias);
  Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *Region_new(PyTypeObject *type, PyObject *args,
                            PyObject *kwds) {
  RegionObject *self;
  self = (RegionObject *)type->tp_alloc(type, 0);
  if (self != NULL) {
    self->name = PyUnicode_FromString("");
    if (self->name == NULL) {
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
  }

  return (PyObject *)self;
}

static int Region_init(RegionObject *self, PyObject *args, PyObject *kwds) {
  static char *kwlist[] = {"name", NULL};
  PyObject *name = NULL, *tmp;

  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|O", kwlist, &name))
    return -1;

  self->__identity__ = atomic_fetch_add(&region_identity, 1);

  if (name) {
    tmp = self->name;
    Py_INCREF(name);
    self->name = name;
    Py_XDECREF(tmp);
  } else {
    self->name = PyUnicode_FromFormat("region_%llu", self->__identity__);
    if (self->name == NULL) {
      Py_DECREF(self);
      return NULL;
    }
  }

  return 0;
}

static PyObject *Region_getname(RegionObject *self, void *closure) {
  Py_INCREF(self->name);
  return self->name;
}

static int Region_setname(RegionObject *self, PyObject *value, void *closure) {
  PyObject *tmp;
  if (value == NULL) {
    PyErr_SetString(PyExc_TypeError, "Cannot delete the name attribute");
    return -1;
  }

  if (!PyUnicode_Check(value)) {
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

static PyObject *Region_getidentity(RegionObject *self, void *closure) {
  return PyLong_FromUnsignedLongLong(self->__identity__);
}

static int Region_setidentity(RegionObject *self, PyObject *value,
                              void *closure) {
  PyErr_SetString(PyExc_TypeError, "Cannot set the __identity__ attribute");
  return -1;
}

static PyObject *Region_getregion(RegionObject *self, void *closure) {
  Py_INCREF(self->__region__);
  return self->__region__;
}

static int Region_setregion(RegionObject *self, PyObject *value,
                            void *closure) {
  PyErr_SetString(PyExc_TypeError, "Cannot set the __region__ attribute");
  return -1;
}

static PyObject *Region_getisopen(RegionObject *self, void *closure) {
  return PyBool_FromLong(self->is_open);
}

static int Rego_setisopen(RegionObject *self, PyObject *value, void *closure) {
  PyErr_SetString(PyExc_TypeError, "Cannot set the is_open attribute");
  return -1;
}

static PyObject *Region_getisshared(RegionObject *self, void *closure) {
  return PyBool_FromLong(self->is_shared);
}

static int Region_setisshared(RegionObject *self, PyObject *value,
                              void *closure) {
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

static PyObject *Region_str(RegionObject *self, PyObject *Py_UNUSED(ignored)) {
  return PyUnicode_FromFormat("Region(%S<%d>)", self->name, self->__identity__);
}

static PyMethodDef Region_methods[] = {
    {NULL} /* Sentinel */
};

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
};

static PyModuleDef veronapymodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "veronapy",
    .m_doc = "veronapy is a Python extension that adds Behavior-oriented "
             "Concurrency runtime for Python.",
    .m_size = -1,
};

PyMODINIT_FUNC PyInit_veronapy(void) {
  PyObject *m;
  if (PyType_Ready(&RegionType) < 0)
    return NULL;

  m = PyModule_Create(&veronapymodule);
  if (m == NULL)
    return NULL;

  PyModule_AddStringConstant(m, "__version__", "0.0.1");

  Py_INCREF(&RegionType);
  if (PyModule_AddObject(m, "region", (PyObject *)&RegionType) < 0) {
    Py_DECREF(&RegionType);
    Py_DECREF(m);
    return NULL;
  }

  return m;
}