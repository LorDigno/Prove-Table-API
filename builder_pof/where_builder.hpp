#ifndef WHERE_BUILDER_HPP
#define WHERE_BUILDER_HPP

#include <string>
#include <functional>
#include <windflow.hpp>

//funtore che applica la logica della where
//tranne che per le firme (e l'operatore istanziato) identico a quello della Select
template <typename T>    
class Where_Functor {
private:
    std::function<bool(const T&)> filt_func;

public:
    //costruttore che associa la lambda
    Where_Functor(std::function<bool(const T&)> func) 
        : filt_func(func) {}   

    //metodo chiamato dal filter   
    bool operator()(T& input) const {
        return filt_func(input);
    }
};

//vero e proprio builder che gestisce i parametri e istanzia la Map
template <typename T>
class Where_Builder {
private:
    std::function<bool(const T&)> filt_func;
    std::string op_name = "Where_Operator";
    size_t parallelism = 1;

public:
    //associa la lambda
    Where_Builder(std::function<bool(const T&)> func)
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

    //istanzia, con tutti i parametri dati, sul Select_Functor e rende la Map nativa 
    auto build() {
        Where_Functor<T> functor(filt_func);
        return wf::Filter_Builder(functor)
            .withName(op_name)
            .withParallelism(parallelism)
            .build();
    }
};

#endif