#ifndef TAGGED_TUPLE_HPP
#define TAGGED_TUPLE_HPP

//wrapper per le tuple con tag di provenienza
template <typename T>
struct Tagged_Tuple {
    T& data;
    int tag; // 0 = left, 1 = right

    Tagged_Tuple() = default;
    Tagged_Tuple(const T& d, int t) : data(d), tag(t) {}
};

#endif