#ifndef GLOBAL_GROUP_BUILDER_HPP
#define GLOBAL_GROUP_BUILDER_HPP

#include <string>
#include <functional>
#include <windflow.hpp>

//funtore che applica la logica della reduce
template <typename InputT, typename OutputT>
class Global_Group_Functor{
    private:
        std::function<void(const InputT&, OutputT&)> agg_func;

    public:
        //costruttore che associa la lambda
        Global_Group_Functor(std::function<void(const InputT&, OutputT&)> func) 
            : agg_func(func) {} 

        //metodo chiamato dalla reduce    
        void operator()(const InputT& input, OutputT& output){
            //la trasformazione è racchiusa nella lambda.
            //il builder non fa altro che cambiare lo stato tramite la lambda.
            agg_func(input, output);
        }    
};



//vero e proprio builder che gestisce i parametri e istanzia la Reduce/KeyedWinndows
template <typename InputT, typename OutputT, typename KeyT = std::string>
class Global_Group_Builder {
private:
    bool keyed = false;
    std::string op_name = "GroupBy_Operator";
    size_t parallelism = 1;

    //funzione d'aggregazione obbligatoria
    std::function<void(const InputT&, OutputT&)> agg_func;

    //funzione d'estrazione della chiave
    std::function<KeyT(const InputT&)> key_func;

public:
    Global_Group_Builder(std::function<void(const InputT&, OutputT&)> func)
        : agg_func(func) {}

    //da un nome all'operatore di Reduce/KeyedWindows 
    Global_Group_Builder& withName(const std::string& name) {
        this->op_name = name;
        return *this;
    }

    //stabilisce il parallelismo per l'operatore di Reduce/KeyedWindows
    Global_Group_Builder& withParallelism(size_t par) {
        this->parallelism = par;
        return *this;
    }

    //stabilisce la funzione d'estrazione della chiave
    Global_Group_Builder& withKeyBy(std::function<KeyT(const InputT&)> k_func) {
        this->key_func = k_func;
        this->keyed = true;
        return *this;
    }

    auto build(){
        //aggregazione globale via Reduce
        Global_Group_Functor<InputT, OutputT> functor(agg_func);

        auto effective_key_func = keyed ? key_func : [](const InputT&) { return KeyT{}; };
        size_t effective_parallelism = keyed ? parallelism : 1;

        return wf::Reduce_Builder(functor)
            .withName(op_name)
            .withParallelism(effective_parallelism)
            .withKeyBy(effective_key_func)
            .build();
    }
};

#endif