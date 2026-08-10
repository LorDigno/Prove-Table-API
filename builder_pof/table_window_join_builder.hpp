#ifndef TABLE_WINDOW_JOIN_BUILDER_HPP
#define TABLE_WINDOW_JOIN_BUILDER_HPP

#include <string>
#include <functional>
#include <chrono>
#include <optional>
#include <cassert>
#include <windflow.hpp>

template <typename InputT, typename OutputT>
class Join_Functor {
public:
    using tuple_t = InputT;
    using result_t = OutputT;

private:
    std::function<OutputT(const InputT&, const InputT&)> join_func;

public:
    Join_Functor(std::function<OutputT(const InputT&, const InputT&)> func) 
        : join_func(func) {} 

    std::optional<OutputT> operator()(const InputT& left, const InputT& right) {
        return join_func(left, right);
    }    
};

template <typename InputT, typename OutputT, typename KeyT = std::string>
class Table_Window_Join_Builder {
private:
    std::function<OutputT(const InputT&, const InputT&)> join_func;
    std::string op_name = "WindowJoin_Operator";
    size_t parallelism = 1;

    bool keyed = false;
    std::function<KeyT(const InputT&)> key_func;

    bool windowed = false;
    uint64_t win_size = 0;
    uint64_t win_slide = 0;

public:
    Table_Window_Join_Builder(std::function<OutputT(const InputT&, const InputT&)> func)
        : join_func(func) {}

    Table_Window_Join_Builder& withName(const std::string& name) {
        this->op_name = name;
        return *this;
    }

    Table_Window_Join_Builder& withParallelism(size_t par) {
        this->parallelism = par;
        return *this;
    }

    Table_Window_Join_Builder& withKeyBy(std::function<KeyT(const InputT&)> k_func) {
        this->key_func = k_func;
        this->keyed = true;
        return *this;
    }

    Table_Window_Join_Builder& withTBWindow(uint64_t size, uint64_t slide = 0) {
        this->win_size = size;
        this->win_slide = (slide == 0) ? size : slide;
        this->windowed = true;
        return *this;
    }

    auto build() {
        assert(windowed && "Errore: Impossibile creare Window_Join_Operator senza definire una finestra! Invocare conTBWindow().");

        auto effective_key_func = keyed ? key_func : [](const InputT&) { return KeyT{}; };
        size_t effective_parallelism = keyed ? parallelism : 1;

        Join_Functor<InputT, OutputT> functor(join_func);

        auto builder = wf::Window_Join_Builder(functor)
            .withName(op_name)
            .withParallelism(effective_parallelism)
            .withKeyBy(effective_key_func)
            .withKPMode();

        if (win_size == win_slide) {
            //tumble window
            return builder
                .withTumblingWindows(std::chrono::microseconds(win_size))
                .build();
        } else {
            //sliding window
            return builder
                .withSlidingWindows(
                    std::chrono::microseconds(win_size), 
                    std::chrono::microseconds(win_slide)
                )
                .build();
        }
    }
};

#endif