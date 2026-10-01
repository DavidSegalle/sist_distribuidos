<script setup lang="ts">
import { ref, onMounted } from 'vue'
import Payment from './components/Payment.vue'

// Holds the raw value typed in the input
const inputNumber = ref<number | null>(null)

// Holds the confirmed number to pass to the Payment component
const activePaymentId = ref<number | null>(null)

// Holds the fetched payment lists. 
// Note: The API returns dictionaries (objects), not arrays.
const paymentStats = ref<{
  active: Record<string, string>
  accepted: Record<string, string>
  rejected: Record<string, string>
} | null>(null)

const API_BASE_URL = 'http://localhost:8001'

// Triggered when the user clicks the button or presses Enter
const loadPayment = () => {
  if (inputNumber.value !== null && !isNaN(inputNumber.value)) {
    activePaymentId.value = inputNumber.value
  }
}

// Fetch the current state of all payments
const fetchPaymentStats = async () => {
  try {
    const response = await fetch(`${API_BASE_URL}/get_active`)
    if (response.ok) {
      paymentStats.value = await response.json()
    } else {
      console.error('Failed to fetch payment stats')
    }
  } catch (error) {
    console.error('Error fetching payment stats:', error)
  }
}

// Called when the Payment component emits 'close'
const handleClosePayment = () => {
  activePaymentId.value = null
  fetchPaymentStats() // Refresh the lists after a payment action
}

// Load stats when the component mounts
onMounted(() => {
  fetchPaymentStats()
})
</script>

<template>
  <main>
    <!-- Small box to type the number -->
    <div class="search-box">
      <input 
        v-model.number="inputNumber" 
        type="number" 
        placeholder="Type a number..." 
        @keyup.enter="loadPayment" 
      />
      <button @click="loadPayment">Fetch</button>
      <button @click="fetchPaymentStats" class="refresh-btn" title="Refresh Lists">↻</button>
    </div>

    <!-- Renders Payment only when a valid number is submitted -->
    <Payment 
      v-if="activePaymentId" 
      :payment-id="activePaymentId" 
      :key="activePaymentId" 
      @close="handleClosePayment"
    />

    <!-- Payment Stats Lists -->
    <div v-if="paymentStats" class="payment-stats">
      <!-- Active Column -->
      <div class="stats-column">
        <h3>Active</h3>
        <ul>
          <li v-for="(status, id) in paymentStats.active" :key="id">
            <span class="payment-id">ID: {{ id }}</span>
            <button 
              class="view-btn" 
              @click="inputNumber = Number(id); loadPayment()"
            >
              View
            </button>
          </li>
          <li v-if="Object.keys(paymentStats.active).length === 0" class="empty">None</li>
        </ul>
      </div>
      
      <!-- Accepted Column -->
      <div class="stats-column">
        <h3>Accepted</h3>
        <ul>
          <li v-for="(status, id) in paymentStats.accepted" :key="id">
            <span class="payment-id">ID: {{ id }}</span>
          </li>
          <li v-if="Object.keys(paymentStats.accepted).length === 0" class="empty">None</li>
        </ul>
      </div>

      <!-- Rejected Column -->
      <div class="stats-column">
        <h3>Rejected</h3>
        <ul>
          <li v-for="(status, id) in paymentStats.rejected" :key="id">
            <span class="payment-id">ID: {{ id }}</span>
          </li>
          <li v-if="Object.keys(paymentStats.rejected).length === 0" class="empty">None</li>
        </ul>
      </div>
    </div>

    <TheWelcome />
  </main>
</template>

<style scoped>
main {
  padding: 2rem;
}

/* Styles for the search box */
.search-box {
  display: flex;
  gap: 0.5rem;
  margin-bottom: 1.5rem;
  max-width: 400px;
}

.search-box input {
  flex: 1;
  padding: 0.5rem;
  border: 1px solid #ccc;
  border-radius: 4px;
  font-size: 1rem;
}

.search-box button {
  padding: 0.5rem 1rem;
  background-color: #42b883;
  color: white;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-size: 1rem;
}

.search-box button:hover {
  background-color: #3aa876;
}

.refresh-btn {
  background-color: #6c757d !important;
  font-weight: bold;
  font-size: 1.2rem !important;
  padding: 0.5rem 0.75rem !important;
}

.refresh-btn:hover {
  background-color: #5a6268 !important;
}

/* Styles for the payment stats lists */
.payment-stats {
  display: flex;
  gap: 1.5rem;
  margin-top: 2rem;
  flex-wrap: wrap;
}

.stats-column {
  flex: 1;
  min-width: 200px;
  background: #f8f9fa;
  padding: 1rem;
  border-radius: 8px;
  border: 1px solid #e9ecef;
}

.stats-column h3 {
  margin-top: 0;
  margin-bottom: 1rem;
  color: #212529;
  border-bottom: 2px solid #42b883;
  padding-bottom: 0.5rem;
  font-size: 1.1rem;
}

.stats-column ul {
  list-style: none;
  padding: 0;
  margin: 0;
}

.stats-column li {
  padding: 0.5rem 0;
  border-bottom: 1px solid #e9ecef;
  display: flex;
  justify-content: space-between;
  align-items: center;
  color: #495057;
  font-size: 0.95rem;
}

.stats-column li:last-child {
  border-bottom: none;
}

.stats-column .empty {
  color: #868e96;
  font-style: italic;
  justify-content: center;
  display: block;
  text-align: center;
}

.payment-id {
  font-weight: 600;
  color: #212529;
}

.view-btn {
  padding: 0.25rem 0.6rem;
  font-size: 0.8rem;
  background-color: #42b883;
  color: white;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  transition: background 0.2s;
}

.view-btn:hover {
  background-color: #3aa876;
}

@media (min-width: 1024px) {
  header {
    display: flex;
    place-items: center;
    padding-right: calc(var(--section-gap) / 2);
  }

  .logo {
    margin: 0 2rem 0 0;
  }

  header .wrapper {
    display: flex;
    place-items: flex-start;
    flex-wrap: wrap;
  }
}
</style>