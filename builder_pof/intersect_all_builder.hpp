#ifndef INTERSECT_ALL_BUILDER_HPP
#define INTERSECT_ALL_BUILDER_HPP

#include <string>
#include <functional>
#include <windflow.hpp>
#include <tagged_tuple.hpp>

//stato per ogni tupla
struct IntersectAll_TupleState {
    int left = 0;
    int right = 0;
    int emitted = 0;
};

template <typename RealT>    
class Intersect_All_Functor {
private:
    //mappa dello stato delle singole tuple
    std::unordered_map<RealT, IntersectAll_TupleState> tuple_state;   

public:
    Intersect_All_Functor() = default;

    void operator()(const Tagged_Tuple<RealT>& in, wf::Shipper<RealT>& shipper) {
        IntersectAll_TupleState& state = tuple_state[in.data]; 

        //aggiorna lo stato in base al flusso di provenienza
        if (in.tag == 0) {
            state.left += 1;
        } else {
            state.right += 1;
        }

        if (state.left > 0 && state.right > 0) {
            state.left += -1;
            state.right += -1;
            shipper.push(in.data);
        }
    }   
};

template <typename RealT>
class Intersect_All_Builder {
private:
    std::string op_name = "Intersect_All_Operator";
    size_t parallelism = 1;

    bool keyed = false;
    std::function<RealT(const Tagged_Tuple<RealT>&)> key_func = [](const Tagged_Tuple<RealT>& in) { return in.data; };

public:
    Intersect_All_Builder() = default;

    //da un nome all'operatore
    Intersect_All_Builder& withName(const std::string& name) {
        this->op_name = name;
        return *this;
    }

    //stabilisce il parallelismo per l'operatore
    Intersect_All_Builder& withParallelism(size_t par) {
        this->parallelism = par;
        return *this;
    }

    auto build(){
        Intersect_All_Functor<RealT> functor;
        return wf::FlatMap_Builder(functor)
            .withName(op_name)
            .withParallelism(1)
            .build();    
    }

    auto build_keyed(){
        Intersect_All_Functor<RealT> functor;
        return wf::FlatMap_Builder(functor)
            .withName(op_name)
            .withParallelism(parallelism)
            .withKeyBy(key_func)
            .build();    
    }
};

#endif