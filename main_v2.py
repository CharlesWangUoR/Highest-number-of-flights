import csv
from threading import Thread
from queue import Queue
from collections import defaultdict
from functools import reduce
import time

class MapReduce:
    def __init__(self, chunk_size):
        #  Initializes the MapReduce instance with a given chunk_size and creates a Queue to store the results.
        self.chunk_size = chunk_size
        self.queue = Queue()

    def chunks(self, data):
        #  Divides the input data into chunks of size chunk_size and returns a list of those chunks.
        return [data[i:i + self.chunk_size] for i in range(0, len(data), self.chunk_size)]

    def mapper(self, chunk):
        # Takes a chunk of data as input and counts the number of flights for each passenger.
        flight_counts = defaultdict(int)
        for row in chunk:
            passenger_id = row[0]
            flight_counts[passenger_id] += 1
        self.queue.put(flight_counts)

    def reducer(self, flight_counts, reduced_data):
        # Performs the reduction step by combining flight counts from different mappers
        # print(flight_counts)
        for passenger_id, times in reduced_data.items():
            flight_counts[passenger_id] += times
        return flight_counts

    def run(self, data_chunks):
        threads = []
        # Walk through the list of data blocks, creating a thread for each block to execute the mapper method.
        for chunk in data_chunks:
            thread = Thread(target=self.mapper, args=(chunk,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        reduced_data = []

        while not self.queue.empty():
            reduced_data.append(self.queue.get())

        final_result = reduce(self.reducer, reduced_data)
        # print(final_result)
        passenger_id, highest_flight_count = max(final_result.items(), key=lambda x: x[1])

        print(f"The passenger ID with the highest number of flights: {passenger_id}")
        print(f"The highest number of flights: {highest_flight_count}")


def read_data(file_path):
    # Open the file read-only and return its contents as a list
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        return list(reader)


def main():
    passenger_data = read_data('AComp_Passenger_data_no_error.csv')
    chunk_sizes = [9, 10, 50, 100]  # Different chunk sizes
    execution_times = []
    for chunk_size in chunk_sizes:
        map_reduce = MapReduce(chunk_size=chunk_size)
        data_chunks = map_reduce.chunks(passenger_data)

        start_time = time.time()
        map_reduce.run(data_chunks)
        end_time = time.time()

        execution_time = end_time - start_time
        execution_times.append((chunk_size, execution_time))

    shortest_chunk_size, shortest_execution_time = min(execution_times, key=lambda x: x[1])

    print(f"Shortest Chunk Size: {shortest_chunk_size}")
    print(f"Shortest Execution Time: {shortest_execution_time} seconds")


if __name__ == "__main__":
    main()
