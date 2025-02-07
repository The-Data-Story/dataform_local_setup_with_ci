/**
 * Calculate revenue dynamically, applying a tax rate and discounts.
 * @param {number} baseAmount - The base amount for the calculation.
 * @param {number} taxRate - The applicable tax rate (e.g., 0.2 for 20% tax).
 * @param {number} discount - The discount amount to subtract.
 * @returns {number} The calculated revenue.
 */
function calculateRevenue(baseAmount, taxRate = 0.1, discount = 10.0) {
    return (baseAmount * (1 + taxRate) - discount) || 0;
  }
  
  // Export helper functions
  module.exports = {
    calculateRevenue
  };