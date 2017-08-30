#ifndef CERTAIN_UTILS_SINGLETON_H_
#define CERTAIN_UTILS_SINGLETON_H_

namespace Certain
{

template<typename Type>
class clsSingleton
{
private:
	// No copying allowed
	clsSingleton(const clsSingleton &);
	void operator = (const clsSingleton &);

public:
	clsSingleton() { }
	// compatible with gperftools
	//virtual ~clsSingleton() { }

	static Type *GetInstance()
	{
		static Type oInstance;
		return &oInstance;
	}
};

} // namespace Certain

#endif
