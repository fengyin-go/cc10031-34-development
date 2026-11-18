import { createRouter, createWebHistory } from 'vue-router'

import HealthDashboardView from '@/views/HealthDashboardView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    {
      path: '/report_dashboard',
      name: 'report_dashboard',
      component: HealthDashboardView,
    },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
