#ifndef INTERSECT_BUILDER_HPP
#define INTERSECT_BUILDER_HPP

#include <string>
#include <functional>
#include <windflow.hpp>
#include <tagged_tuple.hpp>

//stato per ogni tupla
struct Intersect_TupleState {
    bool left = 0;
    bool right = 0;
    bool emitted = 0;
};

template <typename RealT>    
class Intersect_Functor {
private:
    //mappa dello stato delle singole tuple
    std::unordered_map<RealT, Intersect_TupleState> tuple_state;   

public:
    Intersect_Functor() = default;

    void operator()(const Tagged_Tuple<RealT>& in, wf::Shipper<RealT>& shipper) {
        Intersect_TupleState& state = tuple_state[in.data]; 

        //tupla già rilasciata
        if (state.emitted) {
            return;
        }

        //aggiorna lo stato in base al flusso di provenienza
        if (in.tag == 0) {
            state.left = true;
        } else {
            state.right = true;
        }

        if (state.left && state.right) {
            state.emitted = true;
            shipper.push(in.data);
        }
    }   
};

template <typename RealT>
class Intersect_Builder {
private:
    std::string op_name = "Intersect_Operator";
    size_t parallelism = 1;

    std::function<RealT(const Tagged_Tuple<RealT>&)> key_func = [](const Tagged_Tuple<RealT>& in) { return in.data; };

public:
    Intersect_Builder() = default;

    //da un nome all'operatore
    Intersect_Builder& withName(const std::string& name) {
        this->op_name = name;
        return *this;
    }

    //stabilisce il parallelismo per l'operatore
    Intersect_Builder& withParallelism(size_t par) {
        this->parallelism = par;
        return *this;
    }

    auto build(){
        Intersect_Functor<RealT> functor;
        return wf::FlatMap_Builder(functor)
            .withName(op_name)
            .withParallelism(1)
            .build();
    }

    auto build_keyed(){
        Intersect_Functor<RealT> functor;
        return wf::FlatMap_Builder(functor)
            .withName(op_name)
            .withParallelism(parallelism)
            .withKeyBy(key_func)
            .build();
    }
};

#endif