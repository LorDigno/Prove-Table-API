#ifndef DISTINCT_BUILDER_HPP
#define DISTINCT_HPP

#include <string>
#include <functional>
#include <windflow.hpp>

//funtore utilizzato dalla filter
template <typename InputT>
class Distinct_Functor{
    private:
        std::unordered_set<InputT> seen;

    public:
        //costruttore vuoto?
        Distinct_Functor() {} 

        //metodo chiamato dalla filter   
        bool operator()(InputT& input){
            std::pair res = seen.insert(input);
            return res.second;
        }    
};

//la chiave di default è tutta la tupla
template <typename InputT, typename KeyT = InputT>
class Distinct_Builder {
private:
    std::string op_name = "Distinct_Operator";
    size_t parallelism = 1;
    std::function<KeyT(const InputT&)> key_func = nullptr;
    bool keyed = false;

public:
    Distinct_Builder() = default;

    Distinct_Builder& withName(const std::string& name) {
        this->op_name = name;
        return *this;
    }

    Distinct_Builder& withParallelism(size_t par) {
        this->parallelism = par;
        return *this;
    }

    Distinct_Builder& withKeyBy(std::function<KeyT(const InputT&)> k_func) {
        this->key_func = k_func;
        this->keyed = true;
        return *this;
    }

    auto build() {
        int effective_parallelism = 1;
        Distinct_Functor<InputT> functor;
        return wf::Filter_Builder(functor)
            .withName(op_name)
            .withParallelism(effective_parallelism)
            .build();
    }

    auto build_keyed() {
        assert(keyed);

        Distinct_Functor<InputT> functor;
        return wf::Filter_Builder(functor)
            .withName(op_name)
            .withParallelism(parallelism)
            .withKeyBy(key_func)
            .build();
    }
};

#endif