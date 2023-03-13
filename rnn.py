import tensorflow as tf
from tensorflow.keras.layers import Embedding, Conv1D, MaxPooling1D, LSTM, Dense, Dropout
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
from sklearn.model_selection import train_test_split

# Load the support ticket data
tickets = ... # list of ticket descriptions
resolution = ... # list of corresponding resolutions

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(tickets, resolution, test_size=0.2)

# Tokenize the text data and convert it to sequences
tokenizer = Tokenizer(num_words=10000)
tokenizer.fit_on_texts(X_train)
X_train = tokenizer.texts_to_sequences(X_train)
X_test = tokenizer.texts_to_sequences(X_test)

# Pad the sequences to a fixed length
max_len = 100
X_train = pad_sequences(X_train, maxlen=max_len)
X_test = pad_sequences(X_test, maxlen=max_len)

# Define the CNN architecture
model_cnn = tf.keras.Sequential([
    Embedding(input_dim=10000, output_dim=32, input_length=max_len),
    Conv1D(filters=64, kernel_size=5, activation='relu'),
    MaxPooling1D(pool_size=2),
    Conv1D(filters=128, kernel_size=5, activation='relu'),
    MaxPooling1D(pool_size=2),
    Conv1D(filters=256, kernel_size=5, activation='relu'),
    MaxPooling1D(pool_size=2),
    LSTM(units=64),
    Dense(units=64, activation='relu'),
    Dropout(0.5),
    Dense(units=1, activation='linear')
])

# Compile the CNN model
model_cnn.compile(optimizer='adam', loss='mean_squared_error', metrics=['mse'])

# Train the CNN model
model_cnn.fit(X_train, y_train, epochs=10, batch_size=32, validation_data=(X_test, y_test))

# Define the RNN architecture
model_rnn = tf.keras.Sequential([
    Embedding(input_dim=10000, output_dim=32, input_length=max_len),
    LSTM(units=64, return_sequences=True),
    LSTM(units=64),
    Dense(units=64, activation='relu'),
    Dropout(0.5),
    Dense(units=1, activation='linear')
])

# Compile the RNN model
model_rnn.compile(optimizer='adam', loss='mean_squared_error', metrics=['mse'])

# Train the RNN model
model_rnn.fit(X_train, y_train, epochs=10, batch_size=32, validation_data=(X_test, y_test))
