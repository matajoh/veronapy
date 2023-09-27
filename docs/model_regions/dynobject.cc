/*****************************************************************************
 * This file contains a model implementation of region tracking.
 * 
 * It uses a union-find data structure to track the regions, and efficiently
 * merge them when necessary. 
 */


#include <map>
#include <string>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <memory>

class ObjectReference;

class region_exception : public std::exception
{
  std::string msg;
public:
  region_exception(std::string msg) : msg(msg) {}
  const char* what() const noexcept override { return msg.c_str(); }
};

class RegionObject
{
  friend class ObjectReference;
  friend class Reference;

private:
  size_t strong_count = 1;
  size_t weak_count = 1;

  size_t id = 0;

  // Bottom two bits are used for status
  //   00 - parent object
  //   1X - region root
  //     10 - implicit region root
  //     11 - explicit region root
  // For region root, the remaining bits represent the max depth of the 
  // region parent tree.
  // For parent object, the remaining bits represent the aligned pointer to the parent object. 
  uintptr_t parent{2};

  std::map<std::string, ObjectReference> fields;
  bool is_region;

  bool is_root() const { return (parent & 0x3) != 0; }
  bool is_explicit() const { return (parent & 0x3) == 0x3; }

  RegionObject* get_parent() const
  {
    assert(!is_root());
    return reinterpret_cast<RegionObject*>(parent);
  }

  size_t get_depth() const
  {
    assert(is_root());
    return parent >> 2;
  }

  void dec_weak_ref()
  {
    std::cout << "Decreasing weak ref count on " << id << std::endl;
    weak_count--;
    if (weak_count == 0)
      delete this;
  }

  void dec_strong_ref()
  {
    std::cout << "Decreasing strong ref count on " << id << std::endl;
    strong_count--;
    if (strong_count == 0)
    {
      // This is the destructor
      fields.clear();
      std::cout << "Destroying object: " << id << std::endl;
      // Allow the object to be deallocated now destructor has run
      dec_weak_ref();
    }
  }

  void inc_strong_ref()
  {
    std::cout << "Increasing strong ref count on " << id << std::endl;
    strong_count++;
  }

  void inc_weak_ref()
  {
    std::cout << "Increasing weak ref count on " << id << std::endl;
    weak_count++;
  }

protected:
  void set_explicit()
  {
    assert(is_root());
    parent |= 0x3;
  }

  RegionObject(bool is_region = false) : is_region(is_region)
  {
    static size_t next_id = 0;
    id = next_id++;
    std::cout << "Creating object: " << id << std::endl;
    if (is_region)
      set_explicit();
  }

  RegionObject* find()
  {
    if (is_root())
      return this;
  
    // Perform grandparent path compression to reduce the depth of the parent
    // pointing tree.
    auto curr = this;
    auto parent = get_parent();
    auto first = parent;
    while (!parent->is_root())
    {
      auto grandparent = parent->get_parent();
      curr->parent = (uintptr_t)grandparent;
      curr = parent;
      parent = grandparent;
    }

    if (parent != first)
    {
      // Fix up reference counting.
      // With grandparent path compression, the first parent on the path loses
      // a reference, and the root gains an extra reference.
      parent->inc_weak_ref();
      first->dec_weak_ref();
    }
    return parent;
  }

  void merge(RegionObject* other)
  {
    RegionObject* this_root = find();
    RegionObject* other_root = other->find();
    // Equal so no need to merge.
    if (this_root == other_root)
      return;

    // Ensure that the merge is always done with the deeper tree as the parent.
    if (this_root->get_depth() < other_root->get_depth())
      std::swap(this_root, other_root);
    
    // Increase depth if the two trees are the same depth.
    if (this_root->get_depth() == other_root->get_depth())
      this_root->parent += 4; // Increment depth (by 1)

    assert(this_root->get_depth() > other_root->get_depth());

    // Determine if the merge is valid.
    if (this_root->is_explicit() && other_root->is_explicit())
    {
      throw region_exception("Merging two explicit regions!");
    }

    // Preserve explicit marker if on smaller side.
    if (other_root->is_explicit())
    {
      this_root->set_explicit();
    }

    other_root->parent = (uintptr_t)this_root;
    this_root->inc_weak_ref();
  }
};

class Reference;

class ObjectReference
{
  friend class Reference;

  ObjectReference(RegionObject* object) : object(object) {}

  // This pointer requires a strong reference to the object.
  RegionObject* object{nullptr};
public:
  ObjectReference() {}

  ObjectReference(const ObjectReference& objref) : object(objref.object)
  {
    if (object->is_region)
    {
      throw region_exception("Copying region constructor - abort");
    }
    if (object)
      object->inc_strong_ref();
  }

  ObjectReference(ObjectReference&& objref) : object(objref.object)
  {
    objref.object = nullptr;
  }

  ObjectReference& operator=(const ObjectReference& other)
  {
    if (other.object->is_region)
    {
      throw region_exception("Copying region assign - abort");
    }
    if (object)
      object->dec_strong_ref();
    object = other.object;
    if (object)
      object->inc_strong_ref();
    return *this;
  }

  ObjectReference& operator=(ObjectReference&& other)
  {
    object = other.object;
    other.object = nullptr;
    return *this;
  }

  Reference operator[](std::string name);

  static ObjectReference create()
  {
    return {new RegionObject()};
  }

  static ObjectReference create_region()
  {
    return {new RegionObject(true)};
  }

  ~ObjectReference()
  {
    if (object)
    {
      object->dec_strong_ref();
      object = nullptr;
    }
  }
};

class Reference
{
  std::string key;
  ObjectReference& object;

public:
  Reference(std::string name, ObjectReference& object) : key(name), object(object) {}

  operator ObjectReference() { return object.object->fields[key]; }

  Reference& operator=(const ObjectReference& other)
  {
    ObjectReference& cell = object.object->fields[key];
    cell = other;
    if (!other.object->is_region)
      object.object->merge(other.object);
    return *this;
  }

  Reference& operator=(ObjectReference&& other)
  {
    ObjectReference& cell = object.object->fields[key];
    cell = std::move(other);
    if (!cell.object->is_region)
      object.object->merge(cell.object);
    return *this;
  }

  // This should be treated as borrowed for efficiency reasons.
  Reference(const Reference& other) = delete;
  Reference& operator=(const Reference& other) = delete;
  Reference(Reference&& other) = delete;
  Reference& operator=(Reference&& other) = delete;
};

Reference ObjectReference::operator[](std::string name)
{
  return {name, *this};
};


int main()
{
  std::cout << "region a" << std::endl;
  auto a = ObjectReference::create_region();
  std::cout << "region b" << std::endl;
  auto b = ObjectReference::create_region();
  std::cout << "object c" << std::endl;
  
  auto c = ObjectReference::create();
  std::cout << "object d" << std::endl;
  auto d = ObjectReference::create();
  std::cout << "object e" << std::endl;
  auto e = ObjectReference::create();
  std::cout << "object f" << std::endl;
  auto f = ObjectReference::create();

  // Merge implicit regions
  std::cout << "c.x = e" << std::endl;
  c["x"] = e;
  // Merge explicit and implicit
  std::cout << "a.x = c" << std::endl;
  a["x"] = c;

  // Merge implicit and explicit
  std::cout << "f.w = a" << std::endl;
  f["w"] = c;

  // Merge explicit and implicit
  std::cout << "b.y = d" << std::endl;
  b["y"] = d;

  // Should already be the same region
  std::cout << "a.y = e" << std::endl;
  a["y"] = e;

  // Nest a region
  std::cout << "a.y = std::move(b)" << std::endl;
  a["y"] = std::move(b);
  
  // Should fail.
  try 
  {
    std::cout << "a.z = d" << std::endl;
    a["z"] = d;
  }
  catch (region_exception& e)
  {
    std::cout << "Expected - Caught exception: " << e.what() << std::endl;
  }

  return 0;
}
