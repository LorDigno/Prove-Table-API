#ifndef TABLE_INTERVAL_JOIN_BUILDER_HPP
#define TABLE_INTERVAL_JOIN_BUILDER_HPP

#include <string>
#include <functional>
#include <chrono>
#include <windflow.hpp>

template <typename InputT, typename OutputT>
class Interval_Functor{
    using tuple_t = InputT;
    using result_t = OutputT;

    private:
        //date le due tuple 
        std::function<OutputT(const InputT&, const InputT&)> join_func;

    public:
        Interval_Functor(std::function<OutputT(const InputT&, const InputT&)> func) 
            : join_func(func) {} 
  
        std::optional<OutputT> operator()(const InputT& left, const InputT& right){
            return join_func(left, right);
        }    
};

template <typename InputT, typename OutputT, typename KeyT = std::string>
class Table_Interval_Join_Builder {
private:
    std::function<OutputT(const InputT&, const InputT&)> join_func;
    std::string op_name = "IntervalJoin_Operator";
    size_t parallelism = 1;

    int64_t lower_bound = 0;
    int64_t upper_bound = 0;

    bool keyed = false;
    std::function<KeyT(const InputT&)> key_func;

public:
    //associa la lambda
    Table_Interval_Join_Builder(
        std::function<OutputT(const InputT&, const InputT&)> func,
        int64_t low,
        int64_t up) 
            : join_func(func), lower_bound(low), upper_bound(up) {}

    //da un nome all'operatore    
    Table_Interval_Join_Builder& withName(const std::string& name) {
        this->op_name = name;
        return *this;
    }

    //stabilisce il parallelismo per l'operatore
    Table_Interval_Join_Builder& withParallelism(size_t par) {
        this->parallelism = par;
        return *this;
    }

    //stabilisce la funzione d'estrazione della chiave
    Table_Interval_Join_Builder& withKeyBy(std::function<KeyT(const InputT&)> k_func) {
        this->key_func = k_func;
        this->keyed = true;
        return *this;
    }

    auto build() {
        auto effective_key_func = keyed ? key_func : [](const InputT&) { return KeyT{}; };
        size_t effective_parallelism = keyed ? parallelism : 1;

        Interval_Functor<InputT, OutputT> functor(join_func);
        return wf::Interval_Join_Builder(functor)
            .withName(op_name)
            .withParallelism(effective_parallelism)
            .withKeyBy(effective_key_func)
            .withKPMode()
            .withBoundaries(
                std::chrono::microseconds(lower_bound), 
                std::chrono::microseconds(upper_bound)
            )
            .build();
    }
};

#endif