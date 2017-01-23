
/** Iterate through all keys and values of an object
 * @param obj the json_object instance
 * @param key the local name for the char* key variable defined in the body
 * @param val the local name for the json_object* object variable defined in
 *            the body
 */
#if defined(__GNUC__) && !defined(__STRICT_ANSI__)

# define json_object_object_foreach_ex(obj,key,val) \
 char *key; struct json_object *val; \
 json_type obj_type = json_object_get_type(obj); \
 if(obj_type == json_type_object) \
 for(struct lh_entry *entry = json_object_get_object(obj)->head; ({ if(entry) { key = (char*)entry->k; val = (struct json_object*)entry->v; } ; entry; }); entry = entry->next )

#else /* ANSI C or MSC */

# define json_object_object_foreach_ex(obj,key,val) \
 char *key; struct json_object *val; struct lh_entry *entry; \
 json_type obj_type = json_object_get_type(obj); \
 if(obj_type == json_type_object) \
 for(entry = json_object_get_object(obj)->head; (entry ? (key = (char*)entry->k, val = (struct json_object*)entry->v, entry) : 0); entry = entry->next)

#endif /* defined(__GNUC__) && !defined(__STRICT_ANSI__) */

