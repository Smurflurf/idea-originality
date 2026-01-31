import { defineConfig } from 'vite';
import path from 'path';

export default defineConfig({
  build: {
    outDir: 'target/classes/static/dist',
    emptyOutDir: true,
    manifest: true,

    rollupOptions: {
      input: {
		main: 'src/main/resources/static/script/boot/main.js', 
		menu: 'src/main/resources/static/script/ui/navigation/menu.js', 
		localization: 'src/main/resources/static/script/core/localization.js', 
		
		style: 'src/main/resources/static/styling/style.css',
		tooltips: 'src/main/resources/static/styling/tooltips.css',
		legal: 'src/main/resources/static/styling/legal.css',
      },
      output: {
        entryFileNames: `[name].js`,
        chunkFileNames: `[name].js`,
        assetFileNames: `[name].[ext]`,
      }
    }
  },

  resolve: {
    alias: [
      { 
        // Regel 1: Finde jeden Import, der mit "/script/" beginnt.
        find: /^\/script\//, 
        // Ersetze ihn durch den absoluten Dateisystem-Pfad zu deinem Skript-Ordner.
        replacement: path.resolve(__dirname, 'src/main/resources/static/script/') + '/'
      },
      { 
        // Regel 2: Finde jeden Import, der mit "/styling/" beginnt.
        find: /^\/styling\//,
        // Ersetze ihn durch den absoluten Dateisystem-Pfad zu deinem Styling-Ordner.
        replacement: path.resolve(__dirname, 'src/main/resources/static/styling/') + '/'
      }
    ]
  }
});