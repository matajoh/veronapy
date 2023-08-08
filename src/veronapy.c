#include "region.h"

static PyTypeObject RegionType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "veronapy.region",
    .tp_doc = PyDoc_STR("Region object"),
    .tp_basicsize = sizeof(RegionObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
};

static PyModuleDef veronapymodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "veronapy",
    .m_doc = "veronapy is a Python extension that adds Behavior-oriented Concurrency runtime for Python.",
    .m_size = -1,
};

PyMODINIT_FUNC
PyInit_veronapy(void)
{
    PyObject *m;
    if (PyType_Ready(&RegionType) < 0)
        return NULL;

    m = PyModule_Create(&veronapymodule);
    if (m == NULL)
        return NULL;

    PyModule_AddStringConstant(m, "__version__", "0.0.1");

    Py_INCREF(&RegionType);
    if (PyModule_AddObject(m, "region", (PyObject *) &RegionType) < 0) {
        Py_DECREF(&RegionType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}