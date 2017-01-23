#ifndef __APP_JSON_OBJECT_H_
#define __APP_JSON_OBJECT_H_

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <string>
#include <assert.h>
#include "json/json.h"
#include "json/json_object_private.h"
#include <algorithm>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <functional>

#define JSON_ASSERT assert(m_obj != NULL)

inline bool operator == (const struct json_object_iterator& obj1, const struct json_object_iterator& obj2){
	return json_object_iter_equal(&obj1, &obj2);
}

inline bool operator != (const struct json_object_iterator& obj1, const struct json_object_iterator& obj2){
	return !json_object_iter_equal(&obj1, &obj2);
}

inline void operator ++ (struct json_object_iterator& obj1){
	json_object_iter_next(&obj1);
}
inline void operator++ (struct json_object_iterator& obj1, int){
	json_object_iter_next(&obj1);
}

class JsonObject{
protected:
	void release(json_object* & p){
	    if(p){
		json_object_put(p);
		//fprintf(stderr, "######## release %p\n", p);
		p = NULL;
	    }
	}
	void refer(json_object* p){
	    if(p){
	    	json_object_get(p);
		//fprintf(stderr, "######## get %p\n", p);
	    }
	}
	void assign(json_object * dst_obj){
		json_object * src_obj = m_obj;
		if(dst_obj == m_obj)
		    return;
		m_obj = dst_obj; 
		refer(m_obj);
		release(src_obj);
	}
	void assign(const JsonObject& obj){
		assign(obj.m_obj);
	}
	void assign(const char* str){
		release(m_obj);
		if(!str)
			return;
		m_obj = json_tokener_parse(str);
	}
	
public:
	json_object * m_obj;
	typedef struct json_object_iterator iterator;
	JsonObject(bool is_array = false){
		if(!is_array)
			m_obj = json_object_new_object(); 
		else
			m_obj = json_object_new_array();
	}
	JsonObject(const char* str): m_obj(NULL){
		assign(str);
	}
	JsonObject(json_object *obj): m_obj(NULL){
		assign(obj);
	}
	JsonObject(const JsonObject& obj): m_obj(NULL){
		assign(obj);
	}
	~JsonObject(){
		release(m_obj);
		m_obj = NULL;
	}
	JsonObject& operator = (const JsonObject& obj){
		assign(obj);
		return *this;
	}
	JsonObject& operator = (const char* value){
		assign(value);
		return *this;
	}
	bool operator == (const char* value){
		if(!value && empty())
			return true;
		if(!value || empty())
			return false;
		const char* cur = json_object_get_string(m_obj);
		return strcmp(cur, value) == 0;
	}
	bool empty(){
		return m_obj == NULL;
	}
	iterator begin(){
		JSON_ASSERT;
		return json_object_iter_begin(m_obj);	
	}
	iterator end(){
		JSON_ASSERT;
		return json_object_iter_end(m_obj); 
	}
	iterator find(const char* key){
		if(empty()) return end();
		iterator itr = end();
		for(itr = begin(); itr != end(); itr++){
			const char* i_key = json_object_iter_peek_name(&itr);
			if(strcmp(key, i_key) == 0)
				return itr;
		}
		return itr;
	}
	JsonObject get(const char* key){
		JsonObject result((json_object*)NULL);
		if(!empty()) 
			result = json_object_object_get(m_obj, key);
		return result;
	}
	bool is_array(){
		return json_object_get_type(m_obj) == json_type_array;
	}

	bool is_object() {
		return json_object_get_type(m_obj) == json_type_object;
	}

	JsonObject obtain(const char* key, bool is_array=false){
		if(empty())
			return JsonObject((json_object*)NULL);
		JsonObject tmp_obj = get(key);
		if(tmp_obj.empty() || is_array && tmp_obj.is_array()==false){
			tmp_obj = JsonObject(is_array);
			add(key, tmp_obj);	
		}
		return tmp_obj;
	}
	// can only be right value, cannot be left value
	JsonObject operator[] (const char* key){
		if(empty())
			return JsonObject((json_object*)NULL);
		JsonObject tmp_obj = get(key);
		return tmp_obj;
	}
	bool add(const char* key, JsonObject object){
		JSON_ASSERT;
		if(is_array()){
			fprintf(stderr, "json array for invalid add key %s\n", key);
			return false;
		}
		if(!key)
			return false;	
		if(object.empty()){
			json_object_object_add(m_obj, key, json_object_new_string(""));
			return true;
		}
		refer(object.m_obj);
		json_object_object_add(m_obj, key, object.m_obj);
		return true;
	}
	bool add(const char* key, const char* value){
		JSON_ASSERT;
		if(is_array()){
			fprintf(stderr, "json array for invalid add key %s\n", key);
			return false;
		}
		if(!key || !value)
			return false;
		json_object_object_add(m_obj, key, json_object_new_string(value));
		return true;
	}
	bool erase(const char* key){
		if(is_array()){
			fprintf(stderr, "json array for invalid erase key %s\n", key);
			return false;
		}
		if(empty()) return false;
		json_object_object_del(m_obj, key);	
		return true;
	}
	JsonObject operator[] (int i){
		if(!is_array()){
			fprintf(stderr, "NOT json array for invalid operator [%d]\n", i);
			return JsonObject((const char*)NULL);
		}
		size_t n_size = size();
		if(n_size == 0 || i >= n_size)
			return JsonObject((json_object*)NULL);
		return JsonObject(json_object_array_get_idx(m_obj, i));
	}
	bool deep_copy(JsonObject obj){
		if(obj.empty())
			return false;
		std::string str = obj.JsonString();
		assign(str.c_str());
	}
	//delete array element
	bool erase(int i){
		if(!is_array()){
			fprintf(stderr, "NOT json array for invalid erase %d\n", i);
			return false;
		}
		size_t n_size = size();
		if(n_size == 0 || i >= n_size)
			return false;

		struct array_list * carray = (m_obj->o).c_array;
		carray->length--;
		if(carray->array[i])
			carray->free_fn(carray->array[i]);
		for(int j = i; j < n_size-1; j++)
			carray->array[j] = carray->array[j+1];
		carray->array[n_size-1] = 0;
		return true;
	}
	bool add(int i, JsonObject obj){
		JSON_ASSERT;
		if(!is_array()){
			fprintf(stderr, "NOT json array for invalid add %d: %s\n", i, obj.ToString().c_str());
			return false;
		}
		if(i < 0)
			return false;
		if(obj.empty()){
			json_object_array_put_idx(m_obj, i, json_object_new_string(""));
			return true;
		}
		refer(obj.m_obj);
		json_object_array_put_idx(m_obj, i, obj.m_obj);
		return true;
	}
	bool add(int i, const char* value){
		JSON_ASSERT;
		if(!is_array()){
			fprintf(stderr, "NOT json array for invalid add %d: %s\n", i, value);
			return false;
		}
		if(i < 0 || !value)
			return false;
		json_object_array_put_idx(m_obj, i, json_object_new_string(value));
		return true;
	}
	bool push_front(JsonObject obj){
		JSON_ASSERT;
		if(!is_array()){
			fprintf(stderr, "NOT json array for invalid push_front : %s\n", obj.ToString().c_str());
			return false;
		}

		int n_size = size();		
		struct array_list * carray = (m_obj->o).c_array;
		if(n_size > 0){
			json_object_array_put_idx(m_obj, n_size, (json_object*)carray->array[n_size-1]);
			for(int j = n_size-2; j >= 0; j--)
				carray->array[j+1] = carray->array[j];
			carray->array[0] = obj.m_obj;
		}
		else
			json_object_array_put_idx(m_obj, 0, obj.m_obj);
			
		refer(obj.m_obj);
		return true;
	}
	bool push_front(const char* value){
		JSON_ASSERT;
		if(!value)return false;
		if(!is_array()){
			fprintf(stderr, "NOT json array for invalid push_front : %s\n", value);
			return false;
		}

		json_object* obj = json_object_new_string(value);
		int n_size = size();
		struct array_list * carray = (m_obj->o).c_array;
		if(n_size > 0){
		    json_object_array_put_idx(m_obj, n_size, (json_object*)carray->array[n_size-1]);
		    for(int j = n_size-2; j >= 0; j--)
			carray->array[j+1] = carray->array[j];
		    carray->array[0] = obj;
		}
		else
		    json_object_array_put_idx(m_obj, 0, m_obj);

		return true;
	}
	bool swap(int i, int j){
		JSON_ASSERT;
		if(!is_array()){
			fprintf(stderr, "NOT json array for invalid swap %d and %d\n", i, j);
			return false;
		}
		if(i >= size() || j >= size()){
			fprintf(stderr, "invalid swap %d and %d, size %d\n", i, j, size());
			return false;
		}
		struct array_list * carray = (m_obj->o).c_array;
		std::swap(carray->array[i], carray->array[j]);
		return true;
	}
	bool reverse(){
		JSON_ASSERT;
		if(!is_array()){
			fprintf(stderr, "NOT json array for invalid reverse\n");
			return false;
		}
		struct array_list * carray = (m_obj->o).c_array;
		int i = 0, j = size()-1;
		while(i < j){
			std::swap(carray->array[i], carray->array[j]);
			i++;
			j--;
		}
		return true;
	}
	bool push_back(JsonObject object){
		JSON_ASSERT;
		if(!is_array()){
			fprintf(stderr, "NOT json array for invalid push_back: %s\n", object.ToString().c_str());
			return false;
		}
		if(object.empty()){
			json_object_array_add(m_obj, json_object_new_string(""));
			return true;
		}
		refer(object.m_obj);
		json_object_array_add(m_obj, object.m_obj);
		return true;
	}
	bool push_back(const char* value){
		JSON_ASSERT;
		if(!is_array()){
			fprintf(stderr, "NOT json array for invalid push_back: %s\n", value);
			return false;
		}
		if(!value) return false;
		json_object_array_add(m_obj, json_object_new_string(value));
		return true;
	}
	void set_array(){
		if(is_array())
			return;
		json_object * array = json_object_new_array();
		release(m_obj);
		m_obj = array;
	}
	bool add_array(const char* key){
		if(!key) return false;
		json_object *oj = json_object_object_get(m_obj, key);
		if(!oj){
			oj = json_object_new_array();
			json_object_object_add(m_obj, key, oj);
			return true;
		}
		return json_object_get_type(oj) == json_type_array;
	}

	//how many array num
	size_t size(){
		if(empty() || !is_array())
			return 0;
		return json_object_array_length(m_obj);	
	}
	std::string ToString(){
		std::string result;
		if(empty())
			return result;
		return json_object_get_string(m_obj);
	}
	std::string JsonString(){
		std::string result;
		if(empty()){
			return result;
		}
		return json_object_to_json_string(m_obj);
	}

	friend bool sort_array(JsonObject obj, std::function<bool (JsonObject, JsonObject)> less);
	friend xmlNodePtr ToXml(JsonObject obj, const char* root_name, xmlNodePtr root, const char* array_item_name);
};

typedef std::pair<std::string, JsonObject> json_pair; 
inline json_pair operator * (const struct json_object_iterator& itr) {
	std::pair<std::string, JsonObject> result;
	result.first = json_object_iter_peek_name(&itr);
	result.second = JsonObject(json_object_iter_peek_value(&itr));
	return result;
}

struct JsonObjectLess{
    std::function<bool (JsonObject, JsonObject)> less;
    bool operator()(json_object* obj1, json_object* obj2){
	return less(obj1, obj2);
    }
};

inline bool sort_array(JsonObject obj, std::function<bool (JsonObject, JsonObject)> less){
    int n_size = obj.size();
    if(obj.empty() || n_size == 0)
	return false;
    JsonObjectLess opt; 
    opt.less = less;
    struct array_list * carray = ((obj.m_obj)->o).c_array;
    json_object** pj = (json_object**)carray->array;
    std::sort(pj, pj+n_size, opt);
    /*
    for(int i = 0; i < n_size; i++){
	json_object* cur = pj[i];
	int j = 0;
	for(j = 0; j < i && opt(pj[j], cur); j++);
	for(int idx = i; idx > j; idx--)
		pj[idx] = pj[idx-1];
	pj[j] = cur;
    }
    */
    return true;
}

inline xmlNodePtr ToXml(json_object* obj, const char* root_name, xmlNodePtr root = NULL, const char* array_item_name = "item"){
    if(!root_name || obj == NULL)
	return NULL;
    json_type obj_type = json_object_get_type(obj);
    xmlNodePtr node = NULL;
    switch(obj_type){
	case json_type_array:
	{
		//if(root == NULL)
		//	node = xmlNewNode(NULL, BAD_CAST root_name);
		//else
		node = xmlNewNode(NULL, BAD_CAST root_name);
		if(root)
			xmlAddChild(root, node);
		int size = json_object_array_length(obj);
		for(int i = 0; i < size; i++){
			json_object * child = json_object_array_get_idx(obj, i);
			ToXml(child, array_item_name, node, array_item_name);
		}
		break;
	}
	case json_type_object:
	{
		node = xmlNewNode(NULL, BAD_CAST root_name);
		if(root)
			xmlAddChild(root, node);
		json_object_object_foreach(obj, key_str, child){
			ToXml(child, key_str, node, array_item_name);
		}
		break;
	}
	default:
	{
	    const char* val_str = json_object_get_string(obj);
	    node = xmlNewChild(root, NULL, BAD_CAST root_name, BAD_CAST val_str);
	    break;
	}
    }

    return node; 
}
	
inline xmlNodePtr ToXml(JsonObject obj, const char* root_name, xmlNodePtr root = NULL, const char* array_item_name = "item"){
   return ToXml(obj.m_obj, root_name, root, array_item_name);
}
#endif
