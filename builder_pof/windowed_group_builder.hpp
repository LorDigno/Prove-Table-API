#ifndef WINDOWED_GROUP_BUILDER_HPP
#define WINDOWED_GROUP_BUILDER_HPP

#include <string>
#include <functional>
#include <windflow.hpp>
#include <chrono>
#include <cassert>

//funtore che applica la logica della keyedWindows
template <typename InputT, typename OutputT>
class Windowed_Group_Functor{
    private:
        std::function<void(const InputT&, OutputT&)> agg_func;

    public:
        //costruttore che associa la lambda
        Windowed_Group_Functor(std::function<void(const InputT&, OutputT&)> func) 
            : agg_func(func) {} 

        //metodo chiamato dalla keyed_windows
        void operator()(const wf::Iterable<InputT> &win, OutputT &out){
            //riazzero lo stato
            out = OutputT{};

            //la trasformazione è racchiusa nella lambda.
            //il builder non fa altro che cambiare lo stato tramite la lambda.
            for(const InputT &input : win){
                agg_func(input, out);
            }
        } 
};

enum class WindowType {TIME_BASED, COUNT_BASED };

//vero e proprio builder che gestisce i parametri e istanzia la Reduce/KeyedWinndows
template <typename InputT, typename OutputT, typename KeyT = std::string>
class Windowed_Group_Builder {
private:
    bool keyed = false, windowed = false;
    std::string op_name = "GroupBy_Operator";
    size_t parallelism = 1;

    //funzione d'aggregazione obbligatoria
    std::function<void(const InputT&, OutputT&)> agg_func;

    //funzione d'estrazione della chiave
    std::function<KeyT(const InputT&)> key_func;

    //parametri per la finestra, se temporale si presume microsecondi
    WindowType win_type = WindowType::TIME_BASED;
    uint64_t win_size = 0;
    uint64_t win_slide = 0;

public:
    Windowed_Group_Builder(std::function<void(const InputT&, OutputT&)> func)
        : agg_func(func) {}

    //da un nome all'operatore di Reduce/KeyedWindows 
    Windowed_Group_Builder& withName(const std::string& name) {
        this->op_name = name;
        return *this;
    }

    //stabilisce il parallelismo per l'operatore di Reduce/KeyedWindows
    Windowed_Group_Builder& withParallelism(size_t par) {
        this->parallelism = par;
        return *this;
    }

    //stabilisce la funzione d'estrazione della chiave
    Windowed_Group_Builder& withKeyBy(std::function<KeyT(const InputT&)> k_func) {
        this->key_func = k_func;
        this->keyed = true;
        return *this;
    }

    //inserisce la finestra temporale presuppone i microsecondi
    Windowed_Group_Builder& withTBWindow(uint64_t size, uint64_t slide = 0) {
        this->win_type = WindowType::TIME_BASED;
        this->win_size = size;
        this->win_slide = (slide == 0) ? size : slide; //default a Tumble
        this->windowed = true;
        
        return *this;
    }

    Windowed_Group_Builder& withCBWindow(uint64_t size, uint64_t slide = 0) {
        this->win_type = WindowType::COUNT_BASED;
        this->win_size = size;
        this->win_slide = (slide == 0) ? size : slide; //default a Tumble
        this->windowed = true;
        
        return *this;
    }

    auto build(){
        assert(windowed && "Errore: Impossibile creare Windowed_Group_Operator senza definire una finestra! Invocare conTBWindow() o conCBWindow().");

        //aggregazione windowed via KeyedWindows
        Windowed_Group_Functor<InputT, OutputT> functor(agg_func);

        auto effective_key_func = keyed ? key_func : [](const InputT&) { return KeyT{}; };

        if (win_type == WindowType::TIME_BASED) {
            return wf::Keyed_Windows_Builder(functor)
                .withName(op_name)
                .withParallelism(parallelism)
                .withKeyBy(effective_key_func)
                .withTBWindows(
                    std::chrono::microseconds(win_size), 
                    std::chrono::microseconds(win_slide)
                )
                .build();
        } else {
            return wf::Keyed_Windows_Builder(functor)
                .withName(op_name)
                .withParallelism(parallelism)
                .withKeyBy(effective_key_func)
                .withCBWindows(win_size, win_slide)
                .build();
        }
    }
};

#endif