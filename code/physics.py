"""Shared QUAX physics: raw i/q bytes -> power spectrum. Single source of truth,
imported by both the Spark streaming job and the golden-reference test."""
import numpy as np

NBINS = 2048
FS = 2_000_000  # Hz, per-channel ADC sample rate
SCANS_PER_FILE = 4096  # 2**23 samples / NBINS, fixed by the DAQ's file size
FREQS = np.fft.fftshift(np.fft.fftfreq(NBINS, d=1 / FS))


def unpack(chunk_bytes):
    """chunk_bytes = equal-length i-channel slice followed by q-channel slice (see producer.py)."""
    half = len(chunk_bytes) // 2
    i = np.frombuffer(chunk_bytes[:half], dtype="<f4")
    q = np.frombuffer(chunk_bytes[half:], dtype="<f4")
    z = i + 1j * q
    n_scans = len(z) // NBINS
    return z[: n_scans * NBINS].reshape(n_scans, NBINS)


def power_spectrum(scans):
    spec = np.fft.fftshift(np.fft.fft(scans, axis=1), axes=1)
    return np.abs(spec) ** 2


def chunk_stats(chunk_bytes):
    """Per-message reduction: (sum, sum-of-squares, n_scans) for this chunk's power spectra."""
    power = power_spectrum(unpack(chunk_bytes))
    return power.sum(axis=0), (power ** 2).sum(axis=0), power.shape[0]


def finalize(total, total_sq, count):
    """Merge accumulated chunk_stats (once count == SCANS_PER_FILE) into mean spectrum + std."""
    mean = total / count
    rms = np.sqrt(total_sq / count - mean ** 2)
    return mean, rms
