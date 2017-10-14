#ifndef CERTAIN_AUTOHELPER_H_
#define CERTAIN_AUTOHELPER_H_

#include "utils/Logger.h"
#include "utils/ObjReusedPool.h"
#include "utils/Random.h"

namespace Certain
{

#define NO_COPYING_ALLOWED(cls) \
	cls(const cls &); \
	void operator = (const cls &);

#define TYPE_GET_SET(type, name, tname) \
	const type &Get##name() { return m_##tname; } \
	void Set##name(const type &tname) { m_##tname = tname; }

#define UINT32_GET_SET(var) TYPE_GET_SET(uint32_t, var, i##var)

#define POINTER_GET_SET(type, name, tname) \
	type *Get##name() { return m_##tname; } \
	void Set##name(type *tname) { m_##tname = tname; }

#define BOOLEN_IS_SET(name) \
	const bool &Is##name() { return m_b##name; } \
	void Set##name(const bool &b##name) { m_b##name = b##name; }

#define RETURN_RANDOM_ERROR_WHEN_IN_DEBUG_MODE() \
	static __thread clsRandom *__poRandom = new clsRandom(uint32_t(pthread_self())); \
	if (__poRandom->Next() % 10 == 0) { \
		return -7999; \
	}

template<typename Type>
class clsAutoDelete
{
private:
    Type *m_pType;

public:
    clsAutoDelete(Type *pType) : m_pType(pType) { }
    ~clsAutoDelete() { delete m_pType, m_pType = NULL; }
};

template<typename Type>
class clsAutoFreeObjPtr
{
private:
    Type *m_pType;
    clsObjReusedPool<Type> *m_poPool;

public:
    clsAutoFreeObjPtr(Type *pType, clsObjReusedPool<Type> *poPool)
        : m_pType(pType), m_poPool(poPool) { }

    ~clsAutoFreeObjPtr()
    {
        if (m_poPool != NULL)
        {
            m_poPool->FreeObjPtr(m_pType);
        }
        else
        {
            delete m_pType, m_pType = NULL;
        }
    }
};

} // namespace Certain

#endif // CERTAIN_AUTOHELPER_H_
