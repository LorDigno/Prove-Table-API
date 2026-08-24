#ifndef WHERE_BUILDER_HPP
#define WHERE_BUILDER_HPP

#include <string>
#include <functional>
#include <windflow.hpp>

//funtore che applica la logica della where
template <typename InputT>    
class Where_Functor {
private:
    std::function<bool(const InputT&)> filt_func;

public:
    //costruttore che associa la lambda
    Where_Functor(std::function<bool(const InputT&)> func) 
        : filt_func(func) {}   

    //metodo chiamato dal filter   
    bool operator()(InputT& input){
        return filt_func(input);
    }
};

//vero e proprio builder che gestisce i parametri e istanzia la Filter
template <typename InputT, typename KeyT = std::string>
class Where_Builder {
private:
    std::function<bool(const InputT&)> filt_func;
    std::string op_name = "Where_Operator";
    size_t parallelism = 1;

    bool keyed = false;
    std::function<KeyT(const InputT&)> key_func;

public:
    //associa la lambda
    Where_Builder(std::function<bool(const InputT&)> func)
        : filt_func(func) {}

    //da un nome all'operatore di Filter 
    Where_Builder& withName(const std::string& name) {
        this->op_name = name;
        return *this;
    }

    //stabilisce il parallelismo per l'operatore di Filter
    Where_Builder& withParallelism(size_t par) {
        this->parallelism = par;
        return *this;
    }

    //stabilisce la funzione d'estrazione della chiave
    Where_Builder& withKeyBy(std::function<KeyT(const InputT&)> k_func) {
        this->key_func = k_func;
        this->keyed = true;
        return *this;
    }

    //istanzia, con tutti i parametri dati, sul Select_Functor e rende la Map nativa 
    auto build() {
        auto effective_key_func = keyed ? key_func : [](const InputT&) { return KeyT{}; };
        size_t effective_parallelism = keyed ? parallelism : 1;

        Where_Functor<InputT> functor(filt_func);
        return wf::Filter_Builder(functor)
            .withName(op_name)
            .withParallelism(effective_parallelism)
            .withKeyBy(effective_key_func)
            .build();
    }
};

#endif