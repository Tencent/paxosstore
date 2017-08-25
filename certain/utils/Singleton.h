
/*
* Tencent is pleased to support the open source community by making PaxosStore available.
* Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
* https://opensource.org/licenses/BSD-3-Clause
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



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
