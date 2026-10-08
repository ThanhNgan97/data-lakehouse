import { Link } from "react-router-dom";
import UserTopNavigation from "../components/UserTopNavigation";

export default function UserSourcePending({ name }) {
  return (
    <div className="min-h-screen bg-slate-50">
      <UserTopNavigation />
      <main className="mx-auto max-w-3xl p-6 sm:p-12">
        <p className="text-sm font-semibold text-blue-700">Nguồn dữ liệu dự kiến</p>
        <h1 className="mt-3 text-3xl font-bold">Kết nối {name}</h1>
        <p className="mt-5 leading-7 text-slate-600">
          Chức năng kết nối {name} chưa khả dụng. Bạn có thể tải file từ máy tính
          hoặc nhập file từ URL để đưa dữ liệu vào hệ thống hiện tại.
        </p>
        <Link to="/user/file" className="mt-6 inline-block rounded-lg bg-blue-600 px-5 py-3 font-semibold text-white transition-colors hover:bg-blue-700 focus-visible:outline focus-visible:outline-2 focus-visible:outline-blue-600 focus-visible:outline-offset-2">
          Đi đến Tải file
        </Link>
      </main>
    </div>
  );
}
