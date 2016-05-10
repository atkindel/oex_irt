generate-raws: scripts/open_tunnel.sh scripts/generate_raws.py scripts/close_tunnel.sh
	./scripts/open_tunnel.sh
	./scripts/generate_raws.py
	./scripts/close_tunnel.sh

fetch-raws: scripts/fetch_raws.sh scripts/add_get_header.sh
	./scripts/fetch_raws.sh
	./scripts/add_get_header.sh
	./scripts/add_meta_header.sh
	./scripts/add_certs_header.sh

parse:
	./src/compute_matrices.py

remote-clean: scripts/remote_clean.sh
	./scripts/remote_clean.sh

local-clean:
	rm -rf ./data/raws
