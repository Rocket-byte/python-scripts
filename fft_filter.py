# Author: Ruslana Kruk
# FFT Filter in Python
# This script applies a Fast Fourier Transform (FFT) based filter to a signal.
# It filters out frequencies outside a specified range.
# The script requires numpy and matplotlib libraries.
# Usage: Call fft_filter with signal, min_freq, max_freq, and sample_rate.

import numpy as np
import matplotlib.pyplot as plt

def fft_filter(signal, min_freq, max_freq, sample_rate):
    """
    Apply FFT-based filter to a signal within specified frequency range.

    :param signal: Input signal.
    :param min_freq: Minimum frequency to retain.
    :param max_freq: Maximum frequency to retain.
    :param sample_rate: Sampling rate of the signal.
    :return: Filtered signal.
    """
    # Perform FFT
    fft_result = np.fft.fft(signal)
    freq = np.fft.fftfreq(len(signal), d=1/sample_rate)

    # Filter frequencies
    fft_result[np.abs(freq) < min_freq] = 0
    fft_result[np.abs(freq) > max_freq] = 0

    # Perform inverse FFT
    filtered_signal = np.fft.ifft(fft_result)
    return filtered_signal

# Example usage
sample_rate = 1000  # Hertz
t = np.linspace(0, 1, sample_rate, endpoint=False)
signal = np.sin(2*np.pi*50*t) + np.sin(2*np.pi*120*t)  # Mixed signal

# Filter out the 120 Hz frequency
filtered_signal = fft_filter(signal, 40, 100, sample_rate)

# Visualization
plt.figure(figsize=(12, 6))
plt.subplot(2, 1, 1)
plt.plot(t, signal)
plt.title("Original Signal")
plt.subplot(2, 1, 2)
plt.plot(t, filtered_signal.real)
plt.title("Filtered Signal")
plt.tight_layout()
plt.show()
