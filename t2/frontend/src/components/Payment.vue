<script setup lang="ts">
import { ref } from 'vue'

const props = defineProps<{
  paymentId: number
}>()

// Emit an event to the parent to close this page
const emit = defineEmits<{
  (e: 'close'): void
}>()

const API_BASE_URL = 'http://localhost:8001'

const loading = ref(false)
const result = ref<string | null>(null)
const error = ref<string | null>(null)

// Called when clicking Pay or Don't Pay
const performPayment = async (action: 'paid' | 'rejected') => {
  loading.value = true
  error.value = null
  result.value = null
  
  try {
    // Call the PATCH endpoint with the chosen action
    const response = await fetch(`${API_BASE_URL}/perform_payment/${props.paymentId}/${action}`, {
      method: 'PATCH'
    })
    
    if (!response.ok) {
      const errData = await response.json()
      throw new Error(errData.detail || 'Failed to perform payment')
    }
    
    // Show success message based on the action
    result.value = action === 'paid' ? 'Payment Accepted!' : 'Payment Rejected.'
  } catch (err) {
    error.value = err instanceof Error ? err.message : 'Failed to perform payment'
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="payment-page">
    <h2>Payment for ID: {{ paymentId }}</h2>
    
    <!-- Show buttons if no result or error yet -->
    <div v-if="!result && !error" class="actions">
      <p>Please confirm your payment:</p>
      <button class="btn pay" @click="performPayment('paid')" :disabled="loading">
        {{ loading ? 'Processing...' : 'Pay' }}
      </button>
      <button class="btn dont-pay" @click="performPayment('rejected')" :disabled="loading">
        {{ loading ? 'Processing...' : 'Don\'t Pay' }}
      </button>
    </div>
    
    <!-- Show success message -->
    <div v-else-if="result" class="status success">
      <p>{{ result }}</p>
      <button class="back-btn" @click="emit('close')">Go Back</button>
    </div>
    
    <!-- Show error message -->
    <div v-else-if="error" class="status error">
      <p>Error: {{ error }}</p>
      <button class="back-btn" @click="emit('close')">Go Back</button>
    </div>
  </div>
</template>

<style scoped>
.payment-page {
  margin-bottom: 2rem;
  padding: 2rem;
  border: 1px solid #ddd;
  border-radius: 8px;
  /* 👇 Changed to pure white and added a shadow */
  background: #ffffff; 
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  text-align: center;
  max-width: 400px;
  
  /* 👇 FORCE DARK TEXT to override any global dark mode styles */
  color: #212529; 
}

/* Ensure headings and paragraphs are explicitly dark */
.payment-page h2 {
  color: #111111;
  margin-bottom: 1rem;
}

.payment-page p {
  color: #333333;
  margin-bottom: 1rem;
}

.actions {
  margin-top: 1.5rem;
  display: flex;
  flex-direction: column;
  gap: 1rem;
  align-items: center;
}

.btn {
  padding: 0.75rem 2rem;
  font-size: 1.1rem;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  color: white;
  width: 200px;
  transition: background 0.2s;
  font-weight: bold;
}

.btn.pay {
  background-color: #42b883;
}
.btn.pay:hover:not(:disabled) {
  background-color: #3aa876;
}

.btn.dont-pay {
  background-color: #e53935;
}
.btn.dont-pay:hover:not(:disabled) {
  background-color: #c62828;
}

.btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.status {
  margin-top: 1.5rem;
  padding: 1rem;
  border-radius: 6px;
}

.success {
  background: #e8f5e9;
  color: #1b5e20; /* Darker green for better contrast */
}

.error {
  background: #ffebee;
  color: #b71c1c; /* Darker red for better contrast */
}

.back-btn {
  margin-top: 1rem;
  padding: 0.5rem 1rem;
  background: #495057;
  color: white;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-weight: bold;
}

.back-btn:hover {
  background: #343a40;
}
</style>