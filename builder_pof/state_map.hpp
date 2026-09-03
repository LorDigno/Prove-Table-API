#ifndef STATE_MAP_HPP
#define STATE_MAP_HPP

#include <chrono>
#include <string>
#include <functional>
#include <list>
#include <unordered_map>
#include <cstdint>

//Ipotetica gestione del TTL e pulizia dello stato incrementale.
template<typename InputT>
class State_Map{
    private: 
        //time to live delle tuple/chiavi supponiamo in microsecondi
        uint64_t ttl;

        //possibilità di farlo un tot alla volta
        //int clean_step; 

        //front = più recente, back = più vecchio
        std::list<InputT> key_list;

        //hashmap key -> tempo
        using ListIt = typename std::list<InputT>::iterator;
        std::unordered_map<InputT, std::pair<uint64_t, ListIt>> map;

    public:
        State_Map(uint64_t ttl_in) //int step
            //clean_step(step)
            : ttl(ttl_in) {}

        //inserisce o aggiorna una tupla.
        //il bool reso indica se la tupla è nuova o meno.
        bool insert(const InputT& tuple, uint64_t time){
            auto it = map.find(tuple);
            bool out = false;

            if (it != map.end()) {
                if(it->second.first + ttl <= time){
                    //la vecchia tupla era la da troppo si riaggiorna e si conta come nuova
                    out = true;
                } 

                //aggiorno il timestamp
                it->second.first = time;

                //sposto l'elemento vecchio in testa
                key_list.splice(key_list.begin(), key_list, it->second.second);
            } else {
                //aggiungo la tupla nuova
                key_list.push_front(tuple);
                map[tuple] = {time, key_list.begin()};
                out = true;
            }

            return out;
        }

        //pulizia incrementale dello stato
        void cleanup(uint64_t time) {
            //versione che elimina tutti quelli con ttl scaduto
            //se si ha step si può fare una scansione parziale della lista 

            //int steps = 0;
            
            while (!key_list.empty()) { //&& steps < clean_step
                InputT& oldest_key = key_list.back();
                auto it = map.find(oldest_key);
                
                if (it->second.first + ttl <= time) {
                    map.erase(it);
                    key_list.pop_back();
                    //steps += 1;
                } else {
                    //uscita anticipata data dall'ordine
                    break;
                }
            }
        }
};        

//utility per il tempo in microsecondi
uint64_t now_micros() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
        ).count()
    );
}

#endif